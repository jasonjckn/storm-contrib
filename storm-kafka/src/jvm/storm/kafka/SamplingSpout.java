package storm.kafka;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SamplingSpout implements IRichSpout {
    IRichSpout _delegate;
    double _emitRatio;
    Random _random = new Random();

    // emitRatio is a number between 0.0 and 1.0
    // 0.7 would indicate that 70% of the delegate stream is emitted.
    public SamplingSpout(IRichSpout delegate, double emitRatio) {
        _delegate = delegate;
        _emitRatio = emitRatio;
        assert _emitRatio < 1.000001 && _emitRatio >= 0.0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        _delegate.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _delegate.getComponentConfiguration();
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        _delegate.open(map, topologyContext, new SpoutOutputCollector(new ISpoutOutputCollector() {

            @Override
            public List<Integer> emit(String streamId, List<Object> values, Object msgId) {
                if(_random.nextDouble() < _emitRatio) {
                    return spoutOutputCollector.emit(streamId, values, msgId);
                } else {
                    return new ArrayList();
                }
            }

            @Override
            public void emitDirect(int i, String s, List<Object> objects, Object o) {
                if(_random.nextDouble() < _emitRatio) {
                    spoutOutputCollector.emitDirect(i, s, objects, o);
                }
            }

            @Override
            public void reportError(Throwable throwable) {
                spoutOutputCollector.reportError(throwable);
            }
        }));
    }

    @Override
    public void close() {
        _delegate.close();
    }

    @Override
    public void activate() {
        _delegate.activate();
    }

    @Override
    public void deactivate() {
        _delegate.deactivate();
    }

    @Override
    public void nextTuple() {
        _delegate.nextTuple();
    }

    @Override
    public void ack(Object o) {
        _delegate.ack(o);
    }

    @Override
    public void fail(Object o) {
        _delegate.fail(o);
    }
}

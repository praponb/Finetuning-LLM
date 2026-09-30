SELECT id, channel, order_date FROM orders WHERE shipping_fee > 32 INTERSECT SELECT id, channel, order_date FROM orders WHERE amount < 440 ORDER BY channel;

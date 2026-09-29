SELECT p.amount, p.status, p.method, o.channel FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE o.store_id = 14 ORDER BY method, channel;

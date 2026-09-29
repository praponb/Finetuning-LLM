SELECT p.id, p.status, o.channel, o.store_id, o.customer_id FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE o.shipping_fee BETWEEN 11 AND 26 ORDER BY channel DESC

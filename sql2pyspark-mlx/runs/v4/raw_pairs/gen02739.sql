SELECT orders.status, COUNT(payments.amount) AS amount_count FROM payments LEFT JOIN orders ON payments.order_id = orders.id GROUP BY orders.status ORDER BY status, amount_count;

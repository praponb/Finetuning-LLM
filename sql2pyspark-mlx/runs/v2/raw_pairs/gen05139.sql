SELECT orders.status, COUNT(CASE WHEN payments.method = 'paypal' THEN 1 END) AS paypal_count, MIN(payments.amount) AS min_amount
FROM payments
INNER JOIN orders ON orders.id = payments.order_id
GROUP BY orders.status;

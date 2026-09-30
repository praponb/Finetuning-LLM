SELECT o.shipping_fee, t.cash_count, t.refunded_count
FROM orders o
JOIN (SELECT order_id, COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.cash_count >= 5;

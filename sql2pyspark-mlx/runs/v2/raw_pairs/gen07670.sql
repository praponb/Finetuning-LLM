WITH order_totals AS (
    SELECT order_id, AVG(amount) avg_amount, SUM(CASE WHEN method = 'card' THEN amount ELSE 0 END) card_amount
    FROM payments
    GROUP BY order_id
)
SELECT orders.order_date, order_totals.avg_amount, order_totals.card_amount
FROM orders
LEFT OUTER JOIN order_totals ON orders.id = order_totals.order_id
WHERE order_totals.card_amount > 34;

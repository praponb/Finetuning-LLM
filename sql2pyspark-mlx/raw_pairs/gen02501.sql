WITH order_totals AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values, MIN(amount) AS min_amount
    FROM payments
    GROUP BY order_id
)
SELECT orders.channel, order_totals.method_values, order_totals.min_amount
FROM orders
JOIN order_totals ON orders.id = order_totals.order_id
WHERE order_totals.min_amount >= 540
ORDER BY channel

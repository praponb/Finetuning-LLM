WITH order_totals AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM payments
    WHERE paid_at >= '2024-01-01'
    GROUP BY order_id
)
SELECT o.customer_id, ot.status_values
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id;

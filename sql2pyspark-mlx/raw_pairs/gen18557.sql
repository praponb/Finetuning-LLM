WITH order_summary AS (
    SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list, SUM(amount) AS total_amount
    FROM payments
    WHERE order_id = 30
    GROUP BY order_id
)
SELECT
    o.discount,
    o.channel,
    os.status_list,
    os.total_amount
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id
ORDER BY discount DESC;

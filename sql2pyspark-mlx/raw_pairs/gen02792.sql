WITH order_totals AS (
    SELECT order_id, SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count, MAX(amount) - MIN(amount) AS amount_range
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.store_id,
    ot.refunded_count,
    ot.amount_range
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id

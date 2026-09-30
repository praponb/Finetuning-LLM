WITH order_totals AS (
    SELECT order_id, MIN(amount) AS min_amount, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS')
    GROUP BY order_id
)
SELECT
    o.id,
    o.store_id,
    ot.min_amount,
    ot.avg_amount
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.avg_amount > 35;

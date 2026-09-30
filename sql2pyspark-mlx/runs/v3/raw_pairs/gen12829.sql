WITH OrderTotals AS (
    SELECT order_id, SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_amount
    FROM payments
    WHERE status IN ('REFUNDED', 'SUCCESS', 'FAILED')
    GROUP BY order_id
)
SELECT o.order_date, o.discount, O.refunded_amount
FROM orders o
JOIN OrderTotals O ON o.id = O.order_id
ORDER BY refunded_amount DESC;

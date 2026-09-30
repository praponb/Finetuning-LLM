WITH payments_by_order AS (
    SELECT order_id, COUNT(*) AS cnt
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.status, pbo.cnt
FROM orders o
LEFT OUTER JOIN payments_by_order pbo ON o.id = pbo.order_id
WHERE pbo.cnt > 8
ORDER BY o.id

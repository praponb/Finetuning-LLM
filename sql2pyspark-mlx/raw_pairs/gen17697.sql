WITH OrderSummary AS (
    SELECT order_id, MIN(paid_at) AS first_paid_at, SUM(amount) AS total_amount
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.shipping_fee,
    O.first_paid_at,
    O.total_amount
FROM orders o
INNER JOIN OrderSummary O ON o.id = O.order_id

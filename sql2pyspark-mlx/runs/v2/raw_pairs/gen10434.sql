WITH customer_summary AS (
    SELECT customer_id, SUM(shipping_fee) AS total_shipping_fee
    FROM orders
    WHERE order_date < '2023-03-01'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.name,
    c.credit_limit,
    cs.total_shipping_fee
FROM customers c
LEFT JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.total_shipping_fee > 200

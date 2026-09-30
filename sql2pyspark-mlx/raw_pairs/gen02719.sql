SELECT
    c.credit_limit,
    c.signup_date,
    t.avg_shipping_fee,
    t.unique_stores
FROM customers c
LEFT JOIN (SELECT customer_id, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee, COUNT(DISTINCT amount) AS unique_stores
    FROM orders
    WHERE status IN ('RETURNED', 'COMPLETED', 'CANCELLED')
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.avg_shipping_fee > 61;

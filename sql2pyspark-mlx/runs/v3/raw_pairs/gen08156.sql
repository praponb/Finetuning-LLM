WITH customer_stats AS (
    SELECT customer_id, ROUND(STDDEV(shipping_fee), 2) stddev_shipping_fee
    FROM orders
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    c.city,
    cs.stddev_shipping_fee
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.stddev_shipping_fee > 79;

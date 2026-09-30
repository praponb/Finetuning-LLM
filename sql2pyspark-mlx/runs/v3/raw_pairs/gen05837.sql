SELECT
    c.is_active,
    QUARTER(c.signup_date) AS signup_quarter,
    SUM(o.amount) AS total_amount,
    MAX(o.shipping_fee) AS max_shipping_fee
FROM orders o
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
WHERE o.store_id = 15
GROUP BY c.is_active, QUARTER(c.signup_date);

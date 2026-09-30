SELECT MONTH(c.signup_date) signup_month, c.is_active, ROUND(STDDEV(o.shipping_fee), 2) stddev_shipping_fee
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.country IN ('SG', 'JP')
GROUP BY MONTH(c.signup_date), c.is_active
HAVING ROUND(STDDEV(o.shipping_fee), 2) > 34;

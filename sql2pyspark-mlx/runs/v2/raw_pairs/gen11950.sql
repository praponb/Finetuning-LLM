SELECT
    customers.city,
    DATE_FORMAT(customers.signup_date, 'yyyy-MM') AS signup_month,
    COUNT(*) AS row_count,
    ROUND((100 * SUM(CASE WHEN orders.status = 'RETURNED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_returned
FROM orders
INNER JOIN customers ON customers.id = orders.customer_id
INNER JOIN stores ON orders.store_id = stores.id
WHERE stores.region = 'North'
GROUP BY customers.city, DATE_FORMAT(customers.signup_date, 'yyyy-MM')

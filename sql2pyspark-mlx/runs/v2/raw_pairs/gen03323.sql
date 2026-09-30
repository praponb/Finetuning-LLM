SELECT
    DATE_FORMAT(stores.opened_date, 'yyyy-MM') AS opened_month,
    stores.city,
    COUNT(CASE WHEN orders.channel = 'mobile' THEN 1 END) AS mobile_count,
    MIN(orders.order_date) AS first_order_date
FROM orders
FULL OUTER JOIN stores ON orders.store_id = stores.id
JOIN customers ON orders.customer_id = customers.id
WHERE MONTH(customers.signup_date) IN (4, 5, 6)
GROUP BY DATE_FORMAT(stores.opened_date, 'yyyy-MM'), stores.city;

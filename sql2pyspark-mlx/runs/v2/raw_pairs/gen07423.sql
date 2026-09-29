SELECT
    orders.id,
    orders.store_id,
    customers.email,
    stores.store_name,
    CASE WHEN customers.credit_limit >= 13400 THEN 'High' WHEN customers.credit_limit >= 2500 THEN 'Medium' ELSE 'Low' END AS credit_limit_tier
FROM orders
FULL OUTER JOIN customers ON orders.customer_id = customers.id
LEFT JOIN stores ON orders.store_id = stores.id
WHERE YEAR(stores.opened_date) = 2024 AND customers.age >= 59;

SELECT
    CASE WHEN customers.age >= 60 THEN 'high' WHEN customers.age >= 29 THEN 'mid' ELSE 'low' END age_bucket,
    MONTH(customers.signup_date) signup_month,
    SORT_ARRAY(COLLECT_SET(orders.status)) status_list,
    ROUND(AVG(orders.shipping_fee), 2) avg_shipping_fee
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.id
WHERE customers.age < 56
GROUP BY CASE WHEN customers.age >= 60 THEN 'high' WHEN customers.age >= 29 THEN 'mid' ELSE 'low' END, MONTH(customers.signup_date);

SELECT
    customers.country,
    COUNT(subscriptions.id) subscription_count,
    SORT_ARRAY(COLLECT_SET(subscriptions.status)) status_list,
    ROUND(AVG(subscriptions.monthly_fee), 2) avg_monthly_fee
FROM customers
LEFT JOIN subscriptions ON subscriptions.customer_id = customers.id
WHERE subscriptions.customer_id IS NULL
GROUP BY customers.country;

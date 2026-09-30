SELECT c.is_active, SORT_ARRAY(COLLECT_SET(s.plan)) AS plan_list
FROM subscriptions s
JOIN customers c ON c.id = s.customer_id
WHERE c.country = 'SG'
GROUP BY c.is_active

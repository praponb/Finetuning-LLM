SELECT c.city, SORT_ARRAY(COLLECT_SET(s.plan)) AS plan_list FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id GROUP BY c.city;

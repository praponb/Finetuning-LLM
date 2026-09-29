SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) plan_values
FROM subscriptions
WHERE start_date > '2025-07-05' OR end_date <= '2023-04-24'
GROUP BY customer_id
ORDER BY plan_values ASC, customer_id DESC

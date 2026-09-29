WITH customer_totals AS (
    SELECT customer_id, MAX(end_date) AS latest_end_date, SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.age, ct.latest_end_date, ct.plan_list
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id
ORDER BY ct.latest_end_date

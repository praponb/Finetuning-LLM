WITH subscriptions_by_customer AS (
    SELECT customer_id, SUM(CASE WHEN plan = 'basic' THEN 1 ELSE 0 END) AS basic_count, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM subscriptions
    GROUP BY customer_id
)
SELECT
    c.email,
    c.country,
    sbc.basic_count,
    sbc.status_list
FROM customers c
JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id
ORDER BY email

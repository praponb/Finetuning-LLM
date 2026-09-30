WITH subscriptions_by_customer AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values
    FROM subscriptions
    WHERE status IN ('PAUSED', 'EXPIRED')
    GROUP BY customer_id
)
SELECT c.name, sbc.plan_values
FROM customers c
LEFT JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id

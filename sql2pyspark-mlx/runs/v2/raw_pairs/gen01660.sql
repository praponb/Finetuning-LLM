WITH subscriptions_by_customer AS (
    SELECT customer_id, SUM(monthly_fee) AS total_monthly_fee, COUNT(DISTINCT monthly_fee) AS unique_plans
    FROM subscriptions
    WHERE plan = 'premium'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.age,
    sbc.total_monthly_fee,
    sbc.unique_plans
FROM customers c
LEFT OUTER JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id
WHERE sbc.total_monthly_fee >= 570;

WITH subscriptions_by_customer AS (
    SELECT customer_id, AVG(monthly_fee) AS avg_monthly_fee, MAX(start_date) AS latest_start_date
    FROM subscriptions
    WHERE plan = 'standard'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.signup_date,
    c.credit_limit,
    sbc.avg_monthly_fee,
    sbc.latest_start_date
FROM customers c
LEFT JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id

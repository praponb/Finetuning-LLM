WITH subscriptions_by_customer AS (
    SELECT customer_id, AVG(monthly_fee) AS avg_monthly_fee, SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) AS paused_count
    FROM subscriptions
    WHERE status IN ('PAUSED', 'EXPIRED')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.signup_date,
    sbc.avg_monthly_fee,
    sbc.paused_count
FROM customers c
JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id
ORDER BY avg_monthly_fee DESC, id DESC

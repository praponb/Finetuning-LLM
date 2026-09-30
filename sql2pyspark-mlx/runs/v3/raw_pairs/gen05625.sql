WITH subscriptions_by_customer AS (
    SELECT customer_id, MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.email,
    c.name,
    sbc.min_monthly_fee
FROM customers c
LEFT JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id
WHERE sbc.min_monthly_fee >= 220;

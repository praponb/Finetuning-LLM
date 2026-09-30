SELECT
    id,
    monthly_fee,
    status,
    end_date,
    INITCAP(TRIM(plan)) AS plan_title
FROM subscriptions
WHERE monthly_fee <= 170 AND customer_id IS NOT NULL AND plan = 'premium'
ORDER BY plan_title DESC;

SELECT status, monthly_fee_count, avg_monthly_fee
FROM (
    SELECT status, COUNT(monthly_fee) AS monthly_fee_count, AVG(monthly_fee) AS avg_monthly_fee
    FROM subscriptions
    WHERE status IN ('PAUSED', 'ACTIVE')
    GROUP BY status
) AS agg
WHERE monthly_fee_count >= 4

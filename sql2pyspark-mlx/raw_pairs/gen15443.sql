SELECT
    plan,
    MIN(monthly_fee) AS min_monthly_fee,
    SUM(CASE WHEN status = 'CANCELLED' THEN monthly_fee ELSE 0 END) AS cancelled_monthly_fee,
    COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
WHERE monthly_fee <= 190 AND monthly_fee >= 110
GROUP BY plan;

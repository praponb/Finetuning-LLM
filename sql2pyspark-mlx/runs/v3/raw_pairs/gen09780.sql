SELECT status, avg_monthly_fee
FROM (
    SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
    FROM subscriptions
    GROUP BY status
) AS agg
WHERE avg_monthly_fee > 22
ORDER BY avg_monthly_fee, status DESC;

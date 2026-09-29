SELECT
    plan,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    SUM(SUM(monthly_fee)) OVER (ORDER BY plan) AS cumulative_monthly_fee
FROM subscriptions
GROUP BY plan
ORDER BY cnt DESC, total_monthly_fee;

SELECT
    plan,
    status,
    SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) AS paused_monthly_fee,
    SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee,
    COUNT(*) AS cnt
FROM subscriptions
GROUP BY plan, status
ORDER BY plan, basic_monthly_fee;

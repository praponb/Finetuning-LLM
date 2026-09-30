SELECT
    plan,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk
FROM subscriptions
WHERE start_date BETWEEN '2023-06-01' AND '2024-08-19'
GROUP BY plan
ORDER BY cnt DESC, plan
LIMIT 5

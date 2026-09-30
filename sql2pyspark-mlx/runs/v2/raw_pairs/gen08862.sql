SELECT
    customer_id,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS monthly_fee_rank
FROM subscriptions
WHERE customer_id IN (1, 26, 35, 38)
GROUP BY customer_id;

SELECT
    id,
    customer_id,
    monthly_fee,
    start_date,
    RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) rnk,
    SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id) running_monthly_fee
FROM subscriptions
ORDER BY running_monthly_fee DESC;

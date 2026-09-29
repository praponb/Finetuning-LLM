SELECT
    id,
    customer_id,
    end_date,
    monthly_fee,
    status,
    SUM(monthly_fee) OVER (ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
    DENSE_RANK() OVER (PARTITION BY YEAR(end_date) ORDER BY monthly_fee ASC) AS monthly_fee_dense_rank
FROM subscriptions
WHERE YEAR(start_date) = 2023;

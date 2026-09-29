SELECT
    id,
    status,
    monthly_fee,
    end_date,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 3) AS monthly_fee_share,
    DENSE_RANK() OVER (ORDER BY end_date DESC) AS end_date_dense_rank
FROM subscriptions

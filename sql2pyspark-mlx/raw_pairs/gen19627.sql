SELECT
    id,
    end_date,
    plan,
    monthly_fee,
    status,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY MONTH(end_date), plan), 3) AS monthly_fee_share,
    MAX(monthly_fee) OVER (PARTITION BY MONTH(end_date), status) AS end_date_month_max_monthly_fee
FROM subscriptions

SELECT *
FROM (
    SELECT QUARTER(start_date) start_quarter, AVG(monthly_fee) avg_monthly_fee
    FROM subscriptions
    GROUP BY QUARTER(start_date)
) agg
WHERE avg_monthly_fee >= 170;

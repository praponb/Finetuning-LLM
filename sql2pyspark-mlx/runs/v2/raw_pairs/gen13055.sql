SELECT *
FROM (
    SELECT QUARTER(end_date) AS end_quarter, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY QUARTER(end_date)
) AS t
WHERE monthly_fee_range >= 42

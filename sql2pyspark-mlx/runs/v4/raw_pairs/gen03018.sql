SELECT *
FROM (
    SELECT QUARTER(start_date) AS start_quarter, COUNT(monthly_fee) AS monthly_fee_count, MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    WHERE end_date >= '2025-10-25'
    GROUP BY QUARTER(start_date)
) AS s
WHERE min_monthly_fee < 110
ORDER BY start_quarter DESC;

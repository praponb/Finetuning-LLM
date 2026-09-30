SELECT *
FROM (
    SELECT status, QUARTER(start_date) AS start_quarter, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY status, QUARTER(start_date)
) AS sub
WHERE monthly_fee_range > 69
ORDER BY status ASC

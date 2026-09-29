SELECT *
FROM (
    SELECT QUARTER(end_date) AS end_quarter, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    WHERE customer_id IN (3, 7, 9)
    GROUP BY QUARTER(end_date)
) AS agg
WHERE monthly_fee_range < 22;

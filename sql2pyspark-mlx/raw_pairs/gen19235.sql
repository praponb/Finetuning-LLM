SELECT
    start_date,
    end_date,
    status,
    DAY(start_date) AS start_date_day,
    CEIL(monthly_fee) AS monthly_fee_whole
FROM subscriptions
WHERE monthly_fee BETWEEN 70 AND 230

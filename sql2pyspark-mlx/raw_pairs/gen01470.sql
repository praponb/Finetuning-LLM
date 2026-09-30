SELECT
    status,
    end_date,
    monthly_fee,
    DATEDIFF(start_date, end_date) / 7 weeks_end_date_to_start_date
FROM subscriptions
WHERE end_date >= '2024-01-01';

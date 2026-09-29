SELECT
    monthly_fee,
    end_date,
    customer_id,
    ADD_MONTHS(start_date, 12) AS start_date_plus_months
FROM subscriptions
ORDER BY start_date_plus_months DESC;

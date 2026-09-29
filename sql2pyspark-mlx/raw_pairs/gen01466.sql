SELECT
    customer_id,
    plan,
    DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
    DATEDIFF(CURRENT_DATE(), end_date) / 365.25 AS years_since_end_date
FROM subscriptions
ORDER BY days_end_date_to_start_date DESC;

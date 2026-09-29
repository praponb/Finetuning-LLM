SELECT
    id,
    customer_id,
    monthly_fee,
    LAST_DAY(start_date) AS start_date_month_end
FROM subscriptions;

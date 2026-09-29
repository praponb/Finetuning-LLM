SELECT
    id,
    plan,
    end_date,
    LAST_DAY(end_date) AS end_date_month_end
FROM subscriptions
WHERE (customer_id IN (7, 10, 14, 23) AND YEAR(end_date) >= 2024) OR status NOT IN ('ACTIVE', 'CANCELLED', 'EXPIRED');

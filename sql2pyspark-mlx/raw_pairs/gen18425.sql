SELECT
    id,
    customer_id,
    plan,
    DATE_ADD(start_date, 11) AS start_date_plus_11d
FROM subscriptions
WHERE monthly_fee < 220 AND plan NOT IN ('enterprise', 'standard', 'basic');

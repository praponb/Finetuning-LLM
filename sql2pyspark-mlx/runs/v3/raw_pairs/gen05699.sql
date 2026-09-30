SELECT
    id,
    segment,
    age,
    credit_limit,
    WEEKOFYEAR(signup_date) AS signup_date_weekofyear,
    CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
FROM customers
WHERE city NOT IN ('Bangkok', 'Tokyo', 'Berlin')
ORDER BY signup_date_weekofyear DESC

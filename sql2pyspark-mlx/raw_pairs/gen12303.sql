SELECT
    id,
    country,
    DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
    CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
FROM customers
WHERE YEAR(signup_date) = 2023 AND credit_limit < 11600 AND credit_limit < 11800

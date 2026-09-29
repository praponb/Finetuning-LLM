SELECT
    id,
    country,
    signup_date,
    is_active,
    CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
    CONCAT(email, ', ', segment) AS email_segment
FROM customers

SELECT is_active, IF(is_active, 'Yes', 'No') AS is_active_label, ADD_MONTHS(signup_date, 1) AS signup_date_plus_months
FROM customers
WHERE age IS NULL OR is_active
ORDER BY signup_date_plus_months;

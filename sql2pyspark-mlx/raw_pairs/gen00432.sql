SELECT
    id,
    is_active,
    city,
    ADD_MONTHS(signup_date, 1) AS signup_date_plus_months
FROM customers
WHERE is_active = FALSE

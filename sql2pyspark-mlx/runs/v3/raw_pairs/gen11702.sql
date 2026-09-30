SELECT
    id,
    age,
    signup_date,
    name,
    ADD_MONTHS(signup_date, 12) AS signup_date_plus_months,
    LOWER(city) AS city_lower
FROM customers
WHERE country = 'SG' AND city != 'Tokyo' AND LENGTH(email) > 14

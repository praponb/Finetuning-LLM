SELECT *, ADD_MONTHS(signup_date, 12) AS signup_date_plus_months, UPPER(city) AS city_upper FROM customers ORDER BY credit_limit DESC, segment, id LIMIT 3;

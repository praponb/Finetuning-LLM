SELECT id, end_date, start_date, ADD_MONTHS(end_date, 1) AS end_date_plus_months FROM subscriptions WHERE YEAR(start_date) >= 2025

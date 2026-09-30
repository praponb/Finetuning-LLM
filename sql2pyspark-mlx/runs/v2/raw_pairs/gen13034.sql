SELECT id, city, DATE_SUB(signup_date, 14) AS signup_date_minus_14d FROM customers WHERE country <> 'UK' AND city IN ('London', 'Singapore');

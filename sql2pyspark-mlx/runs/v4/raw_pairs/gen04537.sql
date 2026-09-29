SELECT id, country, name, DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label FROM customers ORDER BY country;

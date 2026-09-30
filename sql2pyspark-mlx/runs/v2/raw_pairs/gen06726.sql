SELECT id, city, signup_date, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS signup_date_row FROM customers ORDER BY id DESC

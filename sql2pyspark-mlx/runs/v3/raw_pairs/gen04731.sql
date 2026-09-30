SELECT id, segment, signup_date, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS signup_date_row FROM customers;

SELECT id, signup_date, credit_limit, ROW_NUMBER() OVER (PARTITION BY MONTH(signup_date) ORDER BY credit_limit DESC, id) AS credit_limit_row FROM customers ORDER BY id DESC;

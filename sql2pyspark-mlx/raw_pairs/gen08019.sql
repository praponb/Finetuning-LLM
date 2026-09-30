SELECT id, country, signup_date, email FROM (SELECT id, country, signup_date, email, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) rnk FROM customers) ranked WHERE rnk <= 2;

SELECT YEAR(c.signup_date) AS signup_year, COUNT(*) AS row_count FROM accounts a INNER JOIN customers c ON c.id = a.customer_id WHERE a.status IN ('CLOSED', 'OPEN') GROUP BY YEAR(c.signup_date);

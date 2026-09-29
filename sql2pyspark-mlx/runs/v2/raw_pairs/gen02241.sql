WITH top_customers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS rnk FROM customers WHERE credit_limit > 8800) SELECT * FROM top_customers WHERE rnk <= 2;

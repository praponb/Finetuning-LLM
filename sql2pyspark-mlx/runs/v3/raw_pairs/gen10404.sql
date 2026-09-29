WITH top_customers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rnk FROM customers) SELECT id, country, credit_limit FROM top_customers WHERE rnk <= 2

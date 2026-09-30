WITH top_customers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS rnk FROM customers) SELECT id, country, age FROM top_customers WHERE rnk <= 3 ORDER BY id DESC

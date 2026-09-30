SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS rnk FROM products WHERE category = 'Fashion') AS r WHERE rnk = 1

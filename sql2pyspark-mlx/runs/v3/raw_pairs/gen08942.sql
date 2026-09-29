SELECT id, category, cost, brand FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS rnk FROM products WHERE cost >= 660) AS r WHERE rnk <= 2

SELECT * FROM (SELECT id, category, cost, product_name, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS rnk FROM products) AS ranked WHERE rnk <= 5 ORDER BY id DESC

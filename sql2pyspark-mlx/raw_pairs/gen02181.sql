SELECT id, category, price, brand FROM (SELECT id, category, price, brand, RANK() OVER (PARTITION BY category ORDER BY price) AS rnk FROM products) AS ranked WHERE rnk <= 2

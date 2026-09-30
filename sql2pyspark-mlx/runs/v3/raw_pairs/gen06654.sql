SELECT brand, SUM(cost) AS total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk FROM products GROUP BY brand ORDER BY 2 DESC, brand LIMIT 5;

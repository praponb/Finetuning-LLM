SELECT brand, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY brand) AS cumulative_cost FROM products GROUP BY brand ORDER BY brand

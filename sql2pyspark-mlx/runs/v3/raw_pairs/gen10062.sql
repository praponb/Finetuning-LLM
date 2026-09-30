SELECT brand, category, ROUND(STDDEV(cost), 2) AS stddev_cost, AVG(cost) AS avg_cost FROM products GROUP BY 1, 2 HAVING avg_cost >= 510 ORDER BY category ASC, stddev_cost ASC

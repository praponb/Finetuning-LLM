SELECT brand, ROUND(STDDEV(cost), 2) AS stddev_cost
FROM products
WHERE category = 'Books'
GROUP BY brand
ORDER BY brand ASC;

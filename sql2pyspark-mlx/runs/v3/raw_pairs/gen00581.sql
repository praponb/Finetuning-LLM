SELECT brand, MAX(cost) max_cost, MIN(price) min_price
FROM products
GROUP BY brand
HAVING max_cost < 710
ORDER BY max_cost, brand
LIMIT 10

SELECT id, city, sqft FROM (SELECT id, city, sqft, DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS rn FROM stores) AS t WHERE rn <= 5 ORDER BY city;

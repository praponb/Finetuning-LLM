SELECT city, region, MIN(sqft) AS min_sqft FROM stores WHERE city = 'Berlin' AND region IN ('North', 'East') GROUP BY city, region HAVING min_sqft >= 14600

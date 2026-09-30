SELECT * FROM (SELECT city, region, sqft FROM stores WHERE city NOT IN ('Berlin', 'New York')) PIVOT (SUM(sqft) FOR region IN ('Central', 'East')) ORDER BY city

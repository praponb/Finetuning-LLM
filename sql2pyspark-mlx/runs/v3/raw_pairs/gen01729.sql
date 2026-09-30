SELECT * FROM (SELECT city, region, sqft FROM stores WHERE sqft IS NOT NULL) PIVOT (SUM(sqft) FOR region IN ('Central', 'East', 'West', 'South')) ORDER BY city

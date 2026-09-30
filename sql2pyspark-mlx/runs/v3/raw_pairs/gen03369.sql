SELECT * FROM (SELECT city, region, sqft FROM stores) PIVOT (COUNT(sqft) FOR region IN ('East', 'West')) ORDER BY city;

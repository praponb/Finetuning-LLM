SELECT * FROM (SELECT city, region, sqft FROM stores WHERE sqft <> 10430) PIVOT (AVG(sqft) FOR region IN ('North', 'Central', 'West', 'South'))

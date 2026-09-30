SELECT * FROM (SELECT city, region, sqft FROM stores WHERE store_name IS NOT NULL)
PIVOT (COUNT(sqft) FOR region IN ('Central', 'West', 'South', 'North'))

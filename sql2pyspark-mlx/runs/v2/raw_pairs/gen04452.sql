SELECT * FROM (SELECT city, region, sqft FROM stores WHERE region <> 'South') PIVOT (MAX(sqft) FOR region IN ('North', 'Central', 'East'))

SELECT * FROM (SELECT city, region, sqft FROM stores WHERE city NOT IN ('Tokyo', 'Bangkok', 'Singapore')) PIVOT (MAX(sqft) FOR region IN ('Central', 'North', 'West'));

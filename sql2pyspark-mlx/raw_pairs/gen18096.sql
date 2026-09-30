SELECT * FROM (SELECT city, region, sqft FROM stores) src PIVOT (AVG(sqft) AS total, COUNT(1) AS item_count FOR region IN ('East', 'North', 'South'));

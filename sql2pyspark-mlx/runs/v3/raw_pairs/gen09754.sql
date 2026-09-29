SELECT * FROM (SELECT region, city, sqft FROM stores WHERE opened_date BETWEEN '2023-08-01' AND '2024-07-06') PIVOT (SUM(sqft) FOR city IN ('Tokyo', 'Chiang Mai', 'New York', 'Singapore'));

SELECT * FROM (SELECT region, city, sqft FROM stores WHERE opened_date <= '2025-04-27') PIVOT (SUM(sqft) FOR city IN ('Chiang Mai', 'Bangkok', 'New York', 'London'));

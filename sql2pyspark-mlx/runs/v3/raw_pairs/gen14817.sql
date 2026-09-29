SELECT * FROM (SELECT YEAR(opened_date) AS opened_year, city, sqft FROM stores)
PIVOT (MAX(sqft) FOR city IN ('Chiang Mai', 'London', 'New York'));

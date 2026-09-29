SELECT * FROM (SELECT region, city, sqft FROM stores WHERE opened_date > '2023-10-29')
PIVOT (COUNT(sqft) FOR city IN ('Bangkok', 'Berlin', 'London'))

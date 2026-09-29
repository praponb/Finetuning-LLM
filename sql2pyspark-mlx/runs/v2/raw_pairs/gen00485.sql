SELECT * FROM (SELECT YEAR(opened_date) AS opened_year, region, sqft FROM stores) PIVOT (MAX(sqft) FOR region IN ('Central', 'South', 'West'));

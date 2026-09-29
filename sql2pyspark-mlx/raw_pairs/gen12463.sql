SELECT * FROM (SELECT id, region, opened_date, sqft, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS rnk FROM stores) AS r WHERE rnk <= 2 ORDER BY region;

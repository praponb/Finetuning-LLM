SELECT id, city, sqft, region FROM (SELECT id, city, sqft, region, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS rnk FROM stores) AS r WHERE rnk <= 3 ORDER BY city

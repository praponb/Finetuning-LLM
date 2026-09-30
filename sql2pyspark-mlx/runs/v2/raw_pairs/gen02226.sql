SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS pos FROM stores) AS r WHERE pos <= 5 ORDER BY city, pos

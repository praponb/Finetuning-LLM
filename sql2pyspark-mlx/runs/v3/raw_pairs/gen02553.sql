SELECT id, city, opened_date, region FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY opened_date DESC) AS pos FROM stores) AS r WHERE pos <= 3;

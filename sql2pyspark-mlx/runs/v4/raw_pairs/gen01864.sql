SELECT id, city, opened_date FROM (SELECT id, city, opened_date, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS pos FROM stores) AS r WHERE pos = 1

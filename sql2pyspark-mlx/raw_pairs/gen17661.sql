SELECT id, city, opened_date FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY opened_date) AS pos FROM stores) AS ranked WHERE pos = 1 ORDER BY id;

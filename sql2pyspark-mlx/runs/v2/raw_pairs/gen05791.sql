SELECT id, city, opened_date FROM (SELECT id, city, opened_date, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS rn FROM stores) AS t WHERE rn <= 3;

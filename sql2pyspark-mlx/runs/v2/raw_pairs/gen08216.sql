WITH top_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS rn FROM stores) SELECT id, city, opened_date, region FROM top_stores WHERE rn <= 5;

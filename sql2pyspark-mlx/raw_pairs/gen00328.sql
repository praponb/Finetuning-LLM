WITH top_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS row_num FROM stores) SELECT * FROM top_stores WHERE row_num <= 2;

SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS row_num FROM stores) AS ranked WHERE row_num = 1 ORDER BY store_name DESC

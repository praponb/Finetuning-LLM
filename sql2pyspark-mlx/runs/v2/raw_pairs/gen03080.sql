SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS row_num FROM products) AS ranked WHERE row_num <= 3 ORDER BY id DESC, category

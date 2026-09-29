SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) row_num FROM products) ranked WHERE row_num = 1 ORDER BY stock_qty DESC;

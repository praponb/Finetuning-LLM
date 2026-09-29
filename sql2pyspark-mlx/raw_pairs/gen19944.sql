SELECT id, product_name, brand, cost, stock_qty, supplier_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS row_num FROM products) ranked WHERE row_num = 1;

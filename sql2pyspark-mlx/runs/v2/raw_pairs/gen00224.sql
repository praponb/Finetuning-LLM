SELECT id, cost, ROW_NUMBER() OVER (ORDER BY cost, id) AS row_num FROM products;

SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS row_num
FROM products
QUALIFY row_num = 1;

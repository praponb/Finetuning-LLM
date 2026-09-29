SELECT revenue FROM store_sales WHERE MONTH(sale_date) IN (6, 8, 11) EXCEPT SELECT price FROM products;

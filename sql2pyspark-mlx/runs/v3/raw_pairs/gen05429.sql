SELECT sale_date FROM store_sales EXCEPT SELECT order_date FROM orders WHERE discount IS NOT NULL;

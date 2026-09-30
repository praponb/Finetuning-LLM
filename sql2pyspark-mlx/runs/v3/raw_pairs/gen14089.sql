SELECT sale_date, product_id FROM store_sales WHERE revenue < 3000 EXCEPT SELECT sale_date, product_id FROM store_sales WHERE revenue > 3900;

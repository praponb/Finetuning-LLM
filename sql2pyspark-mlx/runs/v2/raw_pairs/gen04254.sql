SELECT id, units, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label, DATE_ADD(sale_date, 19) AS sale_date_plus_19d FROM store_sales WHERE product_id = 5;

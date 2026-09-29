SELECT revenue, product_id, units, DATE_SUB(sale_date, 10) AS sale_date_minus_10d, DATE_ADD(sale_date, 85) AS sale_date_plus_85d FROM store_sales WHERE units >= 23;

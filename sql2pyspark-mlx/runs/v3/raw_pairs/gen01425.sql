select id, units, date_sub(sale_date, 61) as sale_date_minus_61d from store_sales where store_id in (1, 5);

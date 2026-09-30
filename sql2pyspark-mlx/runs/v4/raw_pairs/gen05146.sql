select id, product_id, units, store_id, add_months(sale_date, 12) as sale_date_plus_months from store_sales;

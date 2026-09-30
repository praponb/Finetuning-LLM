select *, month(sale_date) as sale_date_month from store_sales where store_id in (2, 5, 6)

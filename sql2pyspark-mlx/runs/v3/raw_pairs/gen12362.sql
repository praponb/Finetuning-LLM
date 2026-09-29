select id, revenue, year(sale_date) as sale_date_year from store_sales where product_id in (2, 7, 8)

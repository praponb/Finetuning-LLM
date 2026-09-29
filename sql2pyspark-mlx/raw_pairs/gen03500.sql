select revenue, store_id, sale_date, coalesce(units, 0) as units_filled, quarter(sale_date) as sale_date_quarter from store_sales where store_id in (1, 3, 5, 6)

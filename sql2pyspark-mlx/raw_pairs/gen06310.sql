select product_id, month(sale_date) as sale_month, max(units) - min(units) as units_range, max(units) as max_units, avg(units) as avg_units from store_sales group by product_id, month(sale_date)

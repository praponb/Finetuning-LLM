SELECT sale_date, revenue, product_id, ROUND(units * 100, 2) adjusted_units, units + revenue revenue_per_unit FROM store_sales WHERE product_id = 9

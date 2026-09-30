SELECT *, revenue + units AS revenue_per_unit, DAY(sale_date) AS sale_date_day FROM store_sales WHERE product_id IN (3, 4) OR store_id = 4

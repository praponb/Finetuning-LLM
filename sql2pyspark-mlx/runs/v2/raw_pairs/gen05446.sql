SELECT *, CEIL(units) AS units_whole, ADD_MONTHS(sale_date, 6) AS sale_date_plus_months FROM store_sales WHERE product_id IN (2, 3, 7, 10)

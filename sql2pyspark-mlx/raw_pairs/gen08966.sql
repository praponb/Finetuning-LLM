SELECT *, GREATEST(units, revenue) greatest_units_revenue, WEEKOFYEAR(sale_date) sale_date_weekofyear FROM store_sales WHERE MONTH(sale_date) IN (2, 4, 11) AND product_id = 7 ORDER BY 4

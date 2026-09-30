SELECT ss.sale_date, p.cost, p.brand, WEEKOFYEAR(p.launch_date) AS launch_date_weekofyear FROM store_sales ss JOIN products p ON ss.product_id = p.id WHERE ss.product_id IN (8, 9, 10)

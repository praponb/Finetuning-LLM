SELECT id, store_id, LAST_DAY(sale_date) AS sale_date_month_end FROM store_sales WHERE store_id IN (3, 4, 5) AND product_id IN (5, 6, 8) AND YEAR(sale_date) = 2024

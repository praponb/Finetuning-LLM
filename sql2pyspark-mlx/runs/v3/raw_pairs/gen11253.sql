SELECT product_id, store_id, LAST_DAY(sale_date) AS sale_date_month_end FROM store_sales WHERE MONTH(sale_date) IN (3, 4, 11) OR units > 36

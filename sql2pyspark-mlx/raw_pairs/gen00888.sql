SELECT MONTH(sale_date) AS sale_month, MIN(sale_date) AS min_sale_date FROM store_sales WHERE revenue >= 2500 OR product_id IN (3, 6) GROUP BY MONTH(sale_date) ORDER BY min_sale_date

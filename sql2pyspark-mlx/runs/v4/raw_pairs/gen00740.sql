SELECT MONTH(sale_date) AS sale_month, store_id, MAX(sale_date) AS last_sale_date FROM store_sales WHERE units >= 23 GROUP BY MONTH(sale_date), store_id ORDER BY store_id DESC;

SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, product_id, MAX(revenue) AS max_revenue FROM store_sales GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM'), product_id HAVING max_revenue < 3100;

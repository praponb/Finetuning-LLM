SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, MAX(sale_date) AS last_sale_date, COUNT(*) AS row_count
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM');

SELECT store_id, DATE_TRUNC('MONTH', sale_date) AS sale_month, COUNT(*) AS row_count
FROM store_sales
GROUP BY store_id, DATE_TRUNC('MONTH', sale_date)
HAVING row_count > 1
ORDER BY store_id DESC;

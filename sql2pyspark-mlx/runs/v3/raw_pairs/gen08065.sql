SELECT store_id, revenue, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
FROM store_sales
ORDER BY revenue DESC;

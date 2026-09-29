SELECT store_id, DATE_SUB(sale_date, 60) AS sale_date_minus_60d, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label
FROM store_sales
WHERE units = 28 AND YEAR(sale_date) >= 2024 AND units < 21

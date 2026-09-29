SELECT COUNT(*) AS store_sale_count
FROM store_sales
WHERE units != 27 AND YEAR(sale_date) = 2024
ORDER BY store_sale_count DESC;

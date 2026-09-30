SELECT product_id
FROM store_sales
WHERE units BETWEEN 18 AND 30
UNION ALL
SELECT product_id
FROM store_sales
WHERE YEAR(sale_date) = 2024;

SELECT sale_date
FROM store_sales
WHERE product_id = 5
UNION ALL
SELECT sale_date
FROM store_sales
WHERE YEAR(sale_date) >= 2023;

SELECT product_id, MONTH(sale_date) AS sale_month, MIN(sale_date) AS min_sale_date
FROM store_sales
WHERE MONTH(sale_date) IN (1, 3, 4) AND sale_date BETWEEN '2023-03-30' AND '2024-08-01'
GROUP BY 1, 2
ORDER BY product_id DESC

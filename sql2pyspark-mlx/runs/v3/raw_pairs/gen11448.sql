SELECT product_id, COUNT(DISTINCT revenue) AS unique_stores, MAX(sale_date) AS latest_sale_date
FROM store_sales
WHERE sale_date < '2023-12-27'
GROUP BY product_id
ORDER BY unique_stores, latest_sale_date DESC

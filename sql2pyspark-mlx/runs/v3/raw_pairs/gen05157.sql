SELECT product_id, MIN(sale_date) AS min_sale_date
FROM store_sales
GROUP BY product_id
ORDER BY min_sale_date DESC, product_id DESC
LIMIT 3;

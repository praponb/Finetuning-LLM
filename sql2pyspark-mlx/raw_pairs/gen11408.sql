SELECT store_id, product_id, MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE units >= 19 AND units = 14 AND sale_date < '2023-04-25'
GROUP BY store_id, product_id;

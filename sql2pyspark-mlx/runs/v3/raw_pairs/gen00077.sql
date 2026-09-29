SELECT product_id, store_id, MIN(sale_date) first_sale_date
FROM store_sales
WHERE product_id = 10
GROUP BY 1, 2
HAVING MAX(revenue) > 2700
ORDER BY first_sale_date, product_id;

SELECT store_id, MIN(sale_date) AS min_sale_date
FROM store_sales
WHERE product_id IN (3, 7, 8, 10) AND revenue < 3300
GROUP BY store_id
HAVING MIN(revenue) < 1700
ORDER BY min_sale_date DESC, store_id ASC
LIMIT 50;

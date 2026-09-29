SELECT *
FROM store_sales
WHERE revenue BETWEEN 2100 AND 3800 AND units >= 31
ORDER BY product_id DESC, id
LIMIT 5;

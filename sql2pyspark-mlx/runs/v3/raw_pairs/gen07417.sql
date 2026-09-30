SELECT id, units, product_id
FROM store_sales
WHERE units <= (SELECT MIN(units) FROM store_sales)
ORDER BY product_id DESC

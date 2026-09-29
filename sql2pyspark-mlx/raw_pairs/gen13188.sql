SELECT id, revenue, product_id
FROM store_sales
WHERE revenue IS NOT NULL
ORDER BY id DESC, product_id DESC;

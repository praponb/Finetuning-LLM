SELECT sale_date
FROM store_sales
WHERE product_id = 4
INTERSECT
SELECT last_restock
FROM inventory

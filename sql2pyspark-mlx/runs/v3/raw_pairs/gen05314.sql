SELECT id, units
FROM store_sales
WHERE units <> 36
UNION
SELECT id, units
FROM store_sales
WHERE product_id IN (4, 7);

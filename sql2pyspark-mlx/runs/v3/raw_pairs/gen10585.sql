SELECT id, units, store_id
FROM store_sales
WHERE sale_date BETWEEN '2024-02-01' AND '2025-03-07'
UNION ALL
SELECT id, units, store_id
FROM store_sales
WHERE product_id IN (1, 3, 6, 7);

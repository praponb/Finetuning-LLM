SELECT
    i.id,
    i.last_restock,
    ss.units,
    DATE_SUB(i.last_restock, 31) AS last_restock_minus_31d
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock > '2023-12-01' OR i.product_id IN (1, 3, 6, 9)
ORDER BY i.last_restock;

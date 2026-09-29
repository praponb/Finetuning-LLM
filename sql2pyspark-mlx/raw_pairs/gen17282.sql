SELECT
    id,
    product_id,
    last_restock,
    DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
FROM inventory
WHERE store_id IN (1, 2, 6)
ORDER BY product_id DESC, id
LIMIT 5

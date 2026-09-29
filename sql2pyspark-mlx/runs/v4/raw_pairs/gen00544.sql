SELECT
    id,
    product_id,
    last_restock,
    WEEKOFYEAR(last_restock) AS last_restock_weekofyear
FROM inventory
WHERE store_id IN (5, 6)
ORDER BY last_restock_weekofyear DESC, last_restock DESC

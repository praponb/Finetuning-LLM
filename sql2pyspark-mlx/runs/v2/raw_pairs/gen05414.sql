SELECT
    product_id,
    CASE WHEN quantity >= 236 THEN 'high' WHEN quantity >= 88 THEN 'mid' ELSE 'low' END quantity_bucket,
    MIN(last_restock) first_last_restock,
    MIN(quantity) min_quantity
FROM inventory
GROUP BY 1, 2

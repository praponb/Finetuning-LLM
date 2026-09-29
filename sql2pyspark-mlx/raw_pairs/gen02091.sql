SELECT
    id,
    product_id,
    DATE_FORMAT(last_restock, 'yyyy-MM-dd') last_restock_label,
    FLOOR(quantity) quantity_whole
FROM inventory
WHERE store_id IN (1, 2, 3, 4) AND (product_id IN (1, 2, 7) OR store_id = 2)
ORDER BY id DESC

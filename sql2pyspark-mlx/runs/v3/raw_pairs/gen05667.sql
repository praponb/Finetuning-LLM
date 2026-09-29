SELECT
    id,
    quantity,
    store_id,
    DATE_ADD(last_restock, 53) AS last_restock_plus_53d,
    DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label
FROM inventory
WHERE product_id IN (1, 3, 4) AND store_id IN (1, 2, 5, 6)
ORDER BY last_restock_label, id
LIMIT 1;

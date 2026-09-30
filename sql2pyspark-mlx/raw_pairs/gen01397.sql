SELECT
    product_id,
    store_id,
    quantity,
    DATE_SUB(last_restock, 18) last_restock_minus_18d
FROM inventory
WHERE NOT (quantity <= 197 AND last_restock >= '2025-05-01' AND last_restock > '2024-02-16');

SELECT
    i.store_id,
    i.last_restock,
    i.product_id,
    p.stock_qty,
    CONCAT_WS('-', p.product_name, p.category) AS product_name_category_key
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
WHERE p.stock_qty <> 369
ORDER BY product_name_category_key;

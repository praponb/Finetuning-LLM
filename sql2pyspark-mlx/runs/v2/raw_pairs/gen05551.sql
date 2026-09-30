SELECT
    i.id,
    i.quantity,
    i.last_restock,
    i.store_id,
    p.launch_date,
    INITCAP(TRIM(p.category)) AS category_title
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
WHERE i.last_restock < '2025-01-02'
ORDER BY i.last_restock;

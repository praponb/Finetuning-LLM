SELECT
    i.product_id,
    p.cost,
    p.product_name,
    p.supplier_id,
    DATE_FORMAT(i.last_restock, 'yyyy-MM-dd') AS last_restock_label
FROM inventory i
JOIN products p ON p.id = i.product_id
WHERE i.last_restock BETWEEN '2023-03-16' AND '2024-05-06'
ORDER BY p.supplier_id DESC

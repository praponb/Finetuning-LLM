SELECT
    i.store_id,
    i.quantity,
    p.price,
    s.rating,
    DATE_FORMAT(i.last_restock, 'yyyy-MM-dd') AS last_restock_label
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id
WHERE YEAR(i.last_restock) = 2023
ORDER BY last_restock_label;

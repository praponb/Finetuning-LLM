SELECT
    i.quantity,
    i.last_restock,
    s.store_name,
    p.cost,
    p.brand
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id

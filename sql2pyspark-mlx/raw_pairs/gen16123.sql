SELECT
    i.id,
    i.last_restock,
    i.store_id,
    s.sqft,
    s.region,
    p.stock_qty
FROM inventory i
JOIN stores s ON s.id = i.store_id
LEFT JOIN products p ON i.product_id = p.id
WHERE s.sqft BETWEEN 14030 AND 14644 AND i.quantity < 167

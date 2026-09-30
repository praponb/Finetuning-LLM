SELECT
    i.id,
    i.last_restock,
    s.opened_date,
    s.sqft,
    p.category
FROM inventory i
JOIN stores s ON i.store_id = s.id
LEFT JOIN products p ON p.id = i.product_id;

SELECT
    i.id,
    i.product_id,
    i.quantity,
    i.last_restock,
    s.store_name,
    s.opened_date,
    s.sqft
FROM inventory i
LEFT OUTER JOIN stores s ON s.id = i.store_id
WHERE s.region IS NOT NULL AND s.region IN ('North', 'Central', 'West')

SELECT
    i.last_restock,
    i.product_id,
    s.opened_date,
    s.sqft,
    p.category,
    p.launch_date
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
JOIN products p ON p.id = i.product_id
WHERE s.sqft BETWEEN 5415 AND 7121;

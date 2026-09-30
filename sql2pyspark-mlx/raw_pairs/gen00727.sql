SELECT
    i.id,
    i.store_id,
    p.cost,
    s.opened_date
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE i.quantity != 179

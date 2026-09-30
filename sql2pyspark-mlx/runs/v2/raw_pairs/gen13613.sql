SELECT
    i.quantity,
    i.product_id,
    p.cost,
    s.opened_date
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
WHERE i.product_id = 9

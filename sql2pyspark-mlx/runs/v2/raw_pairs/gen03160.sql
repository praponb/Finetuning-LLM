SELECT
    i.id,
    i.quantity,
    i.product_id,
    s.region,
    s.opened_date,
    p.price
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
LEFT OUTER JOIN products p ON i.product_id = p.id
ORDER BY i.quantity, 5 DESC

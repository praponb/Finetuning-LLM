SELECT
    i.id,
    i.last_restock,
    p.price,
    p.cost,
    s.region,
    s.sqft
FROM inventory i
LEFT OUTER JOIN products p ON p.id = i.product_id
INNER JOIN stores s ON i.store_id = s.id
WHERE MONTH(s.opened_date) IN (2, 6, 10) OR MONTH(p.launch_date) IN (6, 7, 8)

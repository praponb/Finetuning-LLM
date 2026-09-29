SELECT
    i.id,
    i.product_id,
    i.quantity,
    s.sqft,
    p.launch_date
FROM inventory i
LEFT OUTER JOIN stores s ON s.id = i.store_id
LEFT OUTER JOIN products p ON i.product_id = p.id
WHERE MONTH(p.launch_date) IN (4, 5, 12)
ORDER BY sqft DESC, id
LIMIT 5;

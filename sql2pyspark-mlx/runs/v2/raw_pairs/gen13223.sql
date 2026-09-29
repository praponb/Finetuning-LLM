SELECT
    i.id,
    i.quantity,
    i.product_id,
    p.product_name,
    p.brand,
    s.city
FROM inventory i
RIGHT OUTER JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN stores s ON s.id = i.store_id
WHERE p.brand IS NOT NULL

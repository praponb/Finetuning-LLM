SELECT
    i.id,
    i.quantity,
    i.product_id,
    s.sqft
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
ORDER BY product_id DESC;

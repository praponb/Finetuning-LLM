SELECT
    i.id,
    i.store_id,
    p.product_name,
    p.brand,
    s.region,
    s.store_name
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id;

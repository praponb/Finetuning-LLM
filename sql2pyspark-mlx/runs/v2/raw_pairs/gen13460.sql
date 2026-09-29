SELECT
    i.product_id,
    i.store_id,
    s.store_name,
    p.supplier_id,
    p.launch_date
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
RIGHT JOIN products p ON i.product_id = p.id;

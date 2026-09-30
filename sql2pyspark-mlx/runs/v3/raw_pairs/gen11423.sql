SELECT
    i.product_id,
    i.store_id,
    p.supplier_id,
    s.opened_date
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
LEFT JOIN stores s ON i.store_id = s.id;

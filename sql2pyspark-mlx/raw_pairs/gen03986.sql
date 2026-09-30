SELECT
    i.id,
    i.store_id,
    s.opened_date,
    s.region,
    p.brand
FROM inventory i
FULL OUTER JOIN stores s ON s.id = i.store_id
FULL OUTER JOIN products p ON p.id = i.product_id;

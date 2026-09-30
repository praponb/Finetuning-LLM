SELECT
    ss.id,
    ss.product_id,
    s.sqft,
    s.opened_date,
    p.cost
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON p.id = ss.product_id;

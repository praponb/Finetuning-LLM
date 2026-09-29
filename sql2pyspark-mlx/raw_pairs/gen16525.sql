SELECT
    ss.units,
    p.cost,
    p.brand,
    s.sqft
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE s.city = 'New York';

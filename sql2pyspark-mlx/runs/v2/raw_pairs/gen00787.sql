SELECT
    ss.units,
    ss.sale_date,
    s.sqft,
    p.cost
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
JOIN products p ON ss.product_id = p.id;

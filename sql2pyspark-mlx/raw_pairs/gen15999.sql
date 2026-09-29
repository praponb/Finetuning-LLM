SELECT
    ss.sale_date,
    ss.units,
    p.cost,
    p.product_name,
    s.sqft
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN stores s ON ss.store_id = s.id
ORDER BY p.product_name DESC;

SELECT
    ss.units,
    p.product_name,
    p.category,
    s.rating,
    ROUND(ss.units, 0) AS units_rounded
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id;

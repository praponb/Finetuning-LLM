SELECT
    ss.store_id,
    ss.units,
    p.cost,
    p.stock_qty,
    s.supplier_name,
    UPPER(s.country) AS country_upper
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE SIZE(p.tags) > 1 AND s.rating BETWEEN 1.8 AND 3.2

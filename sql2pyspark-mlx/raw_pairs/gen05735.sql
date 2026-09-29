SELECT
    ss.units,
    p.cost,
    s.city,
    s.country
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.supplier_id IN (16, 25) OR p.category IN ('Sports', 'Fashion')
ORDER BY cost ASC;

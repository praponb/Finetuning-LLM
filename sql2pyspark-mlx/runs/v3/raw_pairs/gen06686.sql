SELECT supplier_id, category, CASE WHEN brand IN ('Acme', 'Globex') THEN 'Group A' ELSE 'Other' END AS brand_group
FROM products
WHERE stock_qty BETWEEN 287 AND 343 AND price >= 760 AND supplier_id IS NULL
ORDER BY brand_group, supplier_id DESC

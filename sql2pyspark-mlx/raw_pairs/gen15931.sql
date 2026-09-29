SELECT
    ss.id,
    ss.product_id,
    ss.store_id,
    p.supplier_id,
    p.product_name,
    s.store_name
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN stores s ON ss.store_id = s.id
WHERE LOWER(p.product_name) LIKE '%ate%' AND ss.product_id IN (1, 3, 9)
ORDER BY product_id DESC, s.store_name ASC;

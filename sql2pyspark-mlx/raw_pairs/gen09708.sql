SELECT
    ss.id,
    ss.product_id,
    p.stock_qty,
    s.supplier_name,
    SORT_ARRAY(p.tags) AS sorted_tags
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.store_id IN (3, 4, 6) AND s.rating BETWEEN 2.7 AND 3.1

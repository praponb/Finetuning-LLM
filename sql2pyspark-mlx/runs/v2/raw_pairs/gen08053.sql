SELECT
    ss.id,
    ss.product_id,
    p.supplier_id,
    p.price,
    p.category
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
ORDER BY id DESC, p.category

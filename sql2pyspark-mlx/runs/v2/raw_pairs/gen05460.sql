SELECT
    ss.id,
    ss.product_id,
    p.supplier_id,
    TRIM(p.category) AS clean_category
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE SIZE(p.tags) >= 2;

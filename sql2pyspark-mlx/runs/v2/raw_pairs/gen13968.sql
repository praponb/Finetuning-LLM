SELECT
    ss.units,
    p.supplier_id,
    p.cost,
    INITCAP(TRIM(p.category)) AS category_title
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE ss.units < 15

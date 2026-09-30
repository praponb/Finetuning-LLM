SELECT
    p.cost,
    p.brand,
    p.supplier_id,
    ss.revenue,
    ss.store_id
FROM products p
INNER JOIN store_sales ss ON ss.product_id = p.id
ORDER BY supplier_id DESC

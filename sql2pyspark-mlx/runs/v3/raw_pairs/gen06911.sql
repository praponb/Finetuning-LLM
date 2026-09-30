SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    p.category,
    p.brand
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id;

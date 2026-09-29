SELECT
    p.stock_qty,
    ss.sale_date,
    ss.revenue,
    LOWER(p.category) AS category_lower
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id

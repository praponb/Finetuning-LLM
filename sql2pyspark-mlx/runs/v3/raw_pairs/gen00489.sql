SELECT
    ss.id,
    ss.store_id,
    p.cost,
    p.supplier_id,
    p.product_name,
    COALESCE(ss.revenue, 0) AS revenue_filled
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
ORDER BY store_id;

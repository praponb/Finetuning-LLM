SELECT
    ss.id,
    ss.product_id,
    ss.sale_date,
    ss.store_id,
    p.cost
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
WHERE ss.store_id IN (2, 3, 4, 6)
ORDER BY ss.sale_date DESC, cost DESC, id ASC
LIMIT 10;

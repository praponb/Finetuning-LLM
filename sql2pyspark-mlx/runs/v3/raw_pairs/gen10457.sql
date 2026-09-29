SELECT
    ss.id,
    ss.product_id,
    s.city,
    s.store_name,
    p.stock_qty,
    p.category,
    ROUND(p.price, 1) AS price_rounded
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
FULL OUTER JOIN products p ON ss.product_id = p.id
WHERE s.city = 'London';

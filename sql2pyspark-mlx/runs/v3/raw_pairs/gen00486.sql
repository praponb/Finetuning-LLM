SELECT
    ss.id,
    ss.store_id,
    s.store_name,
    p.price,
    DATEDIFF(CURRENT_DATE(), ss.sale_date) AS days_since_sale_date
FROM store_sales ss
FULL OUTER JOIN stores s ON ss.store_id = s.id
LEFT JOIN products p ON ss.product_id = p.id
WHERE ss.store_id IN (1, 5)
ORDER BY id

SELECT
    ss.id,
    ss.revenue,
    p.brand,
    s.store_name,
    s.region,
    ROUND(ss.revenue, 2) AS revenue_rounded
FROM store_sales ss
FULL OUTER JOIN products p ON p.id = ss.product_id
INNER JOIN stores s ON s.id = ss.store_id
WHERE p.product_name IS NOT NULL AND s.region IS NULL
ORDER BY revenue_rounded

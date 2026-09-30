SELECT
    d.brand,
    COUNT(*) AS cnt,
    MAX(s.quantity) AS max_quantity
FROM inventory s
INNER JOIN products d ON s.product_id = d.id
GROUP BY d.brand
ORDER BY max_quantity DESC NULLS LAST

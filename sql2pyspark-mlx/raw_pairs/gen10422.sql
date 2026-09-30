SELECT
    d.category,
    COUNT(*) AS cnt,
    MAX(src.quantity) AS max_quantity
FROM inventory src
JOIN products d ON src.product_id = d.id
GROUP BY ROLLUP (d.category)
ORDER BY category NULLS LAST

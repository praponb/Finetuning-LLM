SELECT
    p.city,
    p.region,
    COUNT(*) AS num_rows,
    MAX(e.quantity) AS max_quantity,
    GROUPING_ID() AS grouping_level
FROM inventory e
JOIN stores p ON e.store_id = p.id
GROUP BY ROLLUP (p.city, p.region)
ORDER BY region DESC

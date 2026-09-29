SELECT
    p.city,
    COUNT(*) AS num_rows,
    MIN(e.quantity) AS min_quantity,
    GROUPING_ID() AS grouping_level
FROM inventory e
LEFT JOIN stores p ON e.store_id = p.id
GROUP BY CUBE (p.city)
ORDER BY num_rows DESC NULLS FIRST;

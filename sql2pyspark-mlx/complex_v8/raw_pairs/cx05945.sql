WITH combined AS (
    -- Stack the branches
    SELECT id AS ref_id, wh_code AS category
    FROM warehouses
    WHERE city IN ('Singapore', 'Bangkok')
      AND UPPER(TRIM(wh_code)) LIKE 'K%'
    UNION ALL
    SELECT id, category
    FROM skus
    WHERE weight_kg BETWEEN 4.7 AND 18.5
      AND ARRAY_CONTAINS(labels, 'fragile')
)
SELECT category, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM combined
GROUP BY category
ORDER BY category

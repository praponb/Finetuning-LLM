WITH combined AS (
    -- Stack the branches
    SELECT 'site' AS record_type, id, commissioned_on
    FROM sites
    UNION ALL
    SELECT 'site_2' AS record_type, id, commissioned_on
    FROM sites
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(commissioned_on) AS latest_commissioned_on
FROM combined
GROUP BY record_type
ORDER BY record_type

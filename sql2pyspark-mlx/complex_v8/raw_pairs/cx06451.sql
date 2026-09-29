WITH all_events AS (
    -- Stack the branches
    SELECT tariff, id AS ref_id
    FROM sites
    WHERE zone = 'Z3'
    UNION ALL
    SELECT site_name, id
    FROM sites
)
SELECT tariff, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM all_events
GROUP BY tariff
ORDER BY tariff

WITH all_events AS (
    -- Stack the branches
    SELECT 'site' AS src, id AS entity_id, commissioned_on
    FROM sites
    WHERE tariff IN ('FLAT', 'EV')
      AND zone = 'Z4'
    UNION ALL
    SELECT 'site_2' AS src, id, commissioned_on
    FROM sites
    WHERE commissioned_on < DATE '2025-05-07'
    UNION ALL
    SELECT 'site_3' AS src, id, commissioned_on
    FROM sites
)
SELECT src, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(commissioned_on) AS latest_commissioned_on
FROM all_events
GROUP BY src
ORDER BY src

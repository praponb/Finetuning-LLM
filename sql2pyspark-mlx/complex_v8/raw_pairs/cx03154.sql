WITH base_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        valid_from + INTERVAL 7 DAYS AS valid_from_plus_7d
    FROM tariff_windows
    WHERE rate BETWEEN 2.2 AND 7.6
),
all_events AS (
    -- Stack the branches
    SELECT id AS entity_id, tariff AS label
    FROM base_tariffs
    WHERE TO_DATE(valid_from_plus_7d) < DATE '2025-02-13'
    UNION ALL
    SELECT id, site_name
    FROM sites
    UNION ALL
    SELECT site_id, quality
    FROM meter_readings
    WHERE quality NOT IN ('BAD', 'OK')
      AND kwh < 85.7
)
SELECT label, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM all_events
GROUP BY label
ORDER BY label

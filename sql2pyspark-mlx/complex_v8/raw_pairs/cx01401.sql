WITH combined AS (
    -- Stack the branches
    SELECT
        'reading' AS source,
        id AS qty,
        quality,
        site_id AS entity_id
    FROM meter_readings
    UNION
    SELECT
        'reading_2' AS source,
        id AS qty,
        quality,
        site_id AS entity_id
    FROM meter_readings
    WHERE voltage BETWEEN 213.5 AND 241.9
      AND kwh <= 81.3
)
SELECT source, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM combined
GROUP BY source
ORDER BY source

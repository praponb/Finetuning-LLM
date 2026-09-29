WITH unioned AS (
    -- Stack the branches
    SELECT site_id AS entity_id, id AS entity_id_2
    FROM meter_readings
    UNION ALL
    SELECT site_id, id
    FROM meter_readings
)
SELECT entity_id, COUNT(*) AS n_rows, SUM(entity_id_2) AS total_entity_id_2
FROM unioned
GROUP BY entity_id
ORDER BY entity_id

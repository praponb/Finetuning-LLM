WITH all_events AS (
    -- Stack the branches
    SELECT capacity AS entity_id, opened_on, 'warehouse' AS src
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
      AND capacity < 11768
    UNION
    SELECT capacity AS entity_id, opened_on, 'warehouse_2' AS src
    FROM warehouses
)
SELECT src, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(opened_on) AS latest_opened_on
FROM all_events
GROUP BY src
ORDER BY src

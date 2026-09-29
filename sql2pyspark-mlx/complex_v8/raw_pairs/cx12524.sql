WITH filtered AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CONCAT_WS('|', city, wh_code) AS city_wh_code_label
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%bk%'
),
combined AS (
    -- Stack the branches
    SELECT id AS entity_id, opened_on AS event_date
    FROM filtered
    UNION ALL
    SELECT capacity AS entity_id, opened_on AS event_date
    FROM warehouses
    WHERE city = 'Singapore'
    UNION ALL
    SELECT id AS entity_id, opened_on AS event_date
    FROM warehouses
)
SELECT entity_id, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM combined
GROUP BY entity_id
ORDER BY entity_id

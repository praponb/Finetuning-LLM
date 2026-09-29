WITH combined AS (
    -- Stack the branches
    SELECT wh_code AS category, opened_on AS event_date, 'warehouse' AS source
    FROM warehouses
    WHERE city <> 'Singapore'
      AND LOWER(wh_code) LIKE '%cn%'
    UNION ALL
    SELECT wh_code, opened_on, 'warehouse_2' AS source
    FROM warehouses
    WHERE capacity >= 11005
)
SELECT source, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM combined
GROUP BY source
ORDER BY source

WITH stacked AS (
    -- Stack the branches
    SELECT 'reading' AS record_type, id AS entity_id, kwh
    FROM meter_readings
    WHERE kwh > 52.4
      AND quality <> 'EST'
    UNION
    SELECT 'site' AS record_type, id, installed_kw
    FROM sites
    WHERE commissioned_on IS NOT NULL
)
SELECT record_type, COUNT(*) AS n_rows, SUM(kwh) AS total_kwh
FROM stacked
GROUP BY record_type
ORDER BY record_type

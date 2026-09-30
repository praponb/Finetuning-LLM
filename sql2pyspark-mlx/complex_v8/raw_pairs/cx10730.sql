WITH all_events AS (
    -- Stack the branches
    SELECT site_id, voltage AS amount
    FROM meter_readings
    UNION ALL
    SELECT id, installed_kw
    FROM sites
)
SELECT site_id, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM all_events
GROUP BY site_id
ORDER BY site_id

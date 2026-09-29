WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to
    FROM tariff_windows
    WHERE tariff = 'FLAT'
      AND valid_to < TIMESTAMP '2025-03-09 05:00:00'
),
unioned AS (
    -- Stack the branches
    SELECT
        'tariff' AS src,
        id AS ref_id,
        valid_from AS event_time,
        rate AS amount
    FROM tariff_windows
    UNION ALL
    SELECT
        'reading' AS src,
        site_id AS ref_id,
        read_at AS event_time,
        voltage AS amount
    FROM meter_readings
)
SELECT src, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(event_time) AS latest_event_time
FROM unioned
GROUP BY src
ORDER BY src

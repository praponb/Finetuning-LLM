WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.voltage) AS prior_voltage_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_12h,
        prior_voltage_12h
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY prior_rows_12h DESC, id DESC) AS recency_rank
        FROM covered
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_rows_12h,
    prior_voltage_12h,
    DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
FROM deduped
ORDER BY id DESC NULLS FIRST, prior_voltage_12h DESC NULLS FIRST

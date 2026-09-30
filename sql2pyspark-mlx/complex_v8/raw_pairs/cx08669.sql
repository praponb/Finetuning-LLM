WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.kwh) AS prior_kwh_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
deduped AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_3d,
        prior_kwh_3d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY prior_rows_3d, id) AS rn
        FROM matched
    ) ranked
    WHERE rn = 1
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_rows_3d,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM deduped s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality = 'OK'
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh
    FROM exploded_detail_readings s
)
SELECT
    id,
    read_at,
    reading_id,
    reading_quality,
    reading_kwh
FROM with_counts
ORDER BY id

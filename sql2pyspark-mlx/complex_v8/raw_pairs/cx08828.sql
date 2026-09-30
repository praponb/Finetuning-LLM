WITH matched AS (
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
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_cume_dist,
        MIN(prior_rows_12h) OVER (PARTITION BY site_id) AS site_id_min_prior_rows_12h,
        ROUND(PERCENT_RANK() OVER (PARTITION BY prior_rows_12h, site_id ORDER BY read_at NULLS LAST, id), 3) AS read_at_percent_rank
    FROM matched
)
SELECT
    id,
    read_at,
    prior_rows_12h,
    read_at_cume_dist
FROM sequenced
ORDER BY id

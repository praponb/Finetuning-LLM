WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.voltage) AS prior_voltage_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_7d,
        prior_voltage_7d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY prior_rows_7d DESC, id DESC) AS row_num
        FROM in_window
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        prior_voltage_7d,
        NTILE(4) OVER (PARTITION BY site_id, prior_rows_7d ORDER BY prior_voltage_7d, id) AS prior_voltage_7d_quartile,
        NTILE(5) OVER w_site_id_prior_rows_7d_by_pri AS prior_voltage_7d_quintile
    FROM latest_per_key
    WINDOW w_site_id_prior_rows_7d_by_pri AS (PARTITION BY site_id, prior_rows_7d ORDER BY prior_voltage_7d, id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_voltage_7d,
    prior_voltage_7d_quartile,
    ROUND(prior_voltage_7d * 1.5, 2) AS prior_voltage_7d_adj,
    DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
FROM with_windows
ORDER BY site_id DESC NULLS FIRST

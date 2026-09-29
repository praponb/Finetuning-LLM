WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.voltage) AS prior_voltage_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_1d,
        prior_voltage_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id DESC) AS rn
        FROM matched
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    site_id,
    prior_rows_1d,
    prior_voltage_1d,
    prior_rows_1d - prior_voltage_1d AS prior_rows_1d_minus_prior_voltage_1d,
    ROUND(prior_voltage_1d * 1.07, 2) AS prior_voltage_1d_adj
FROM latest
ORDER BY prior_rows_1d_minus_prior_voltage_1d DESC, prior_rows_1d NULLS LAST

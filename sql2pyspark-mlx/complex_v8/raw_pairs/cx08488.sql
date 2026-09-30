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
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.prior_rows_1d,
        s.prior_voltage_1d,
        si.tariff AS site_tariff
    FROM matched s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        prior_rows_1d,
        prior_voltage_1d,
        SUM(prior_rows_1d) OVER w_site_id_prior_rows_1d AS site_id_prior_rows_1d_sum_prior_rows_1d,
        ROUND(PERCENT_RANK() OVER (PARTITION BY prior_rows_1d ORDER BY prior_rows_1d), 3) AS prior_rows_1d_percent_rank,
        DENSE_RANK() OVER (PARTITION BY prior_rows_1d ORDER BY prior_rows_1d) AS prior_rows_1d_dense_rank,
        DENSE_RANK() OVER (ORDER BY prior_voltage_1d, id) AS prior_voltage_1d_dense_rank
    FROM with_details_site
    WINDOW w_site_id_prior_rows_1d AS (PARTITION BY site_id, prior_rows_1d)
)
-- Final projection
SELECT DISTINCT prior_rows_1d, prior_rows_1d_dense_rank
FROM with_trends
WHERE prior_rows_1d > 1
ORDER BY prior_rows_1d

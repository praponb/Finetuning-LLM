WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.kwh) AS prior_kwh_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
site_id_stats AS (
    -- Max prior_kwh_7d per site_id
    SELECT site_id, MAX(prior_kwh_7d) AS max_prior_kwh_7d
    FROM joined_ranges
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_kwh_7d,
        ROUND(s.prior_kwh_7d - g.max_prior_kwh_7d, 2) AS prior_kwh_7d_minus_max
    FROM joined_ranges s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_kwh_7d = g.max_prior_kwh_7d
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 237.8
              AND MONTH(m.read_at) IN (2, 3)
        )
),
site_id_stats_2 AS (
    -- Median prior_rows_7d per site_id
    SELECT site_id, PERCENTILE_APPROX(prior_rows_7d, 0.5) AS median_prior_rows_7d
    FROM filtered
    GROUP BY site_id
),
above_average_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_rows_7d,
        ROUND(s.prior_rows_7d - g.median_prior_rows_7d, 2) AS prior_rows_7d_minus_median
    FROM filtered s
    JOIN site_id_stats_2 g ON g.site_id = s.site_id
    WHERE s.prior_rows_7d >= g.median_prior_rows_7d
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, max_prior_kwh_7d, prior_rows_7d_minus_median, median_prior_rows_7d
    FROM above_average_2
) src
UNPIVOT (
    reading FOR measure IN (max_prior_kwh_7d, prior_rows_7d_minus_median, median_prior_rows_7d)
) unp
ORDER BY id, measure

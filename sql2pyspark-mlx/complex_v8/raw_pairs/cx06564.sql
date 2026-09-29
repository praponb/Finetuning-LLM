WITH covered AS (
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
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        prior_rows_7d,
        COUNT(*) OVER (PARTITION BY prior_rows_7d, site_id) AS rows_in_prior_rows_7d_site_id,
        prior_rows_7d - LAG(prior_rows_7d) OVER (PARTITION BY prior_rows_7d ORDER BY prior_kwh_7d DESC, id) AS prior_rows_7d_change,
        ROUND(AVG(prior_rows_7d) OVER w_prior_rows_7d_site_id, 2) AS prior_rows_7d_site_id_avg_prior_rows_7d,
        DENSE_RANK() OVER w_prior_rows_7d_by_prior_kwh_7_2 AS prior_kwh_7d_dense_rank
    FROM covered
    WINDOW w_prior_rows_7d_site_id AS (PARTITION BY prior_rows_7d, site_id), w_prior_rows_7d_by_prior_kwh_7_2 AS (PARTITION BY prior_rows_7d ORDER BY prior_kwh_7d DESC)
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.prior_rows_7d_site_id_avg_prior_rows_7d >= 3.6
)
SELECT
    id,
    site_id,
    prior_rows_7d_site_id_avg_prior_rows_7d,
    prior_kwh_7d_dense_rank,
    ABS(prior_rows_7d_change - -2) AS prior_rows_7d_change_distance
FROM screened
WHERE rows_in_prior_rows_7d_site_id BETWEEN 4 AND 14
ORDER BY prior_kwh_7d_dense_rank DESC, id
LIMIT 10

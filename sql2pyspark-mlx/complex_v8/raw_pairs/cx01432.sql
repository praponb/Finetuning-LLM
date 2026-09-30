WITH matched AS (
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
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM matched
    WHERE prior_kwh_7d > (SELECT AVG(prior_kwh_7d) FROM matched)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        prior_kwh_7d,
        ROUND(AVG(prior_kwh_7d) OVER (PARTITION BY prior_rows_7d, site_id ORDER BY read_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_kwh_7d_avg_7,
        ROUND(prior_rows_7d - AVG(prior_rows_7d) OVER (PARTITION BY site_id), 2) AS prior_rows_7d_vs_site_id_avg,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY prior_rows_7d, site_id ORDER BY read_at ASC, id))) / 60, 1) AS minutes_since_prev_read_at
    FROM with_activity
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_rows_in_site_id)) / 60, 1) AS minutes_since_prev_read_at_2,
        LAG(read_at) OVER (ORDER BY rows_in_site_id, id) AS prev_read_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY minutes_since_prev_read_at ASC, id), 3) AS minutes_since_prev_read_at_percent_rank,
        rows_in_site_id - LAG(rows_in_site_id) OVER (ORDER BY minutes_since_prev_read_at ASC, id) AS rows_in_site_id_change
    FROM windowed
    WINDOW w_all_by_rows_in_site_id AS (ORDER BY rows_in_site_id, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, prior_kwh_7d_avg_7, prior_kwh_7d, minutes_since_prev_read_at, minutes_since_prev_read_at_2
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (prior_kwh_7d_avg_7, prior_kwh_7d, minutes_since_prev_read_at, minutes_since_prev_read_at_2)
) unp

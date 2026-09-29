WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.kwh) AS prior_kwh_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        prior_kwh_12h,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY prior_rows_12h, site_id ORDER BY read_at ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        DENSE_RANK() OVER w_prior_rows_12h_site_id_by_re_2 AS read_at_dense_rank,
        MAX(prior_rows_12h) OVER (PARTITION BY prior_rows_12h, site_id) AS prior_rows_12h_site_id_max_prior_rows_12h,
        RANK() OVER (ORDER BY prior_rows_12h ASC, id) AS prior_rows_12h_rank,
        prior_kwh_12h - LAG(prior_kwh_12h) OVER (PARTITION BY prior_rows_12h, site_id ORDER BY read_at ASC, id) AS prior_kwh_12h_change
    FROM joined_ranges
    WINDOW w_prior_rows_12h_site_id_by_re_2 AS (PARTITION BY prior_rows_12h, site_id ORDER BY read_at ASC)
)
SELECT id, field, metric_value
FROM (
    SELECT id, prior_rows_12h_rank, prior_rows_12h_site_id_max_prior_rows_12h, read_at_dense_rank
    FROM metrics
) src
UNPIVOT (
    metric_value FOR field IN (prior_rows_12h_rank AS m_prior_rows_12h_rank, prior_rows_12h_site_id_max_prior_rows_12h AS prior_rows_12h_site_id_max_prior_rows_12h_v, read_at_dense_rank AS m_read_at_dense_rank)
) unp
ORDER BY id, field

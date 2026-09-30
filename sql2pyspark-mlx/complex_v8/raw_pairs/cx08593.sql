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
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(prior_rows_7d) OVER (PARTITION BY prior_rows_7d, site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_rows_7d,
        ROUND(CUME_DIST() OVER (PARTITION BY prior_rows_7d, site_id ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY prior_rows_7d, site_id ORDER BY read_at DESC) AS read_at_dense_rank,
        ROUND(PERCENT_RANK() OVER w_prior_rows_7d_site_id_by_rea_2, 3) AS read_at_percent_rank,
        NTH_VALUE(prior_kwh_7d, 2) OVER (PARTITION BY prior_rows_7d, site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_kwh_7d,
        DENSE_RANK() OVER (ORDER BY prior_rows_7d NULLS LAST, id) AS prior_rows_7d_dense_rank
    FROM covered
    WINDOW w_prior_rows_7d_site_id_by_rea_2 AS (PARTITION BY prior_rows_7d, site_id ORDER BY read_at DESC)
)
SELECT
    first_prior_rows_7d,
    prior_rows_7d_dense_rank,
    GROUPING_ID() AS gid,
    MAX(read_at) AS last_read_at,
    COUNT(*) AS row_count
FROM metrics
GROUP BY ROLLUP(first_prior_rows_7d, prior_rows_7d_dense_rank)
ORDER BY first_prior_rows_7d NULLS LAST, prior_rows_7d_dense_rank NULLS LAST

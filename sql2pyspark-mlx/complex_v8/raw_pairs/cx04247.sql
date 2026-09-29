WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.ms_played) AS prior_ms_played_3d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(prior_rows_3d - AVG(prior_rows_3d) OVER (PARTITION BY listener_id), 2) AS prior_rows_3d_vs_listener_id_avg,
        NTH_VALUE(prior_rows_3d, 2) OVER (ORDER BY prior_ms_played_3d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_3d,
        ROUND(prior_ms_played_3d / NULLIF(SUM(prior_ms_played_3d) OVER w_listener_id, 0), 4) AS prior_ms_played_3d_share_of_listener_id,
        FIRST_VALUE(played_at) OVER (ORDER BY prior_ms_played_3d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at
    FROM joined_ranges
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        ROUND(STDDEV(prior_rows_3d_vs_listener_id_avg), 2) AS stddev_prior_rows_3d_vs_listener_id_avg,
        MAX(prior_rows_3d) AS max_prior_rows_3d,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_trends
    GROUP BY 1
    HAVING MAX(prior_rows_3d) > 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        played_at_month,
        max_prior_rows_3d,
        distinct_listener_ids,
        distinct_ids,
        COUNT(*) OVER (PARTITION BY distinct_ids) AS rows_in_distinct_ids
    FROM per_group
)
SELECT
    played_at_month,
    distinct_ids,
    rows_in_distinct_ids,
    ROUND(max_prior_rows_3d / NULLIF(distinct_ids, 0), 3) AS max_prior_rows_3d_per_distinct_ids,
    TO_DATE(played_at_month) AS played_at_month_date
FROM with_windows
ORDER BY distinct_ids NULLS LAST

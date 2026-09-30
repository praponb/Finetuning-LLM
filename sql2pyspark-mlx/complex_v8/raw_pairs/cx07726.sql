WITH covered AS (
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
grouped AS (
    -- Summarise per group
    SELECT
        prior_rows_3d,
        PERCENTILE_APPROX(prior_rows_3d, 0.5) AS median_prior_rows_3d,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(played_at) AS first_played_at
    FROM covered
    GROUP BY prior_rows_3d
)
SELECT prior_rows_3d, metric, amount
FROM (
    SELECT prior_rows_3d, distinct_ids, distinct_listener_ids, median_prior_rows_3d
    FROM grouped
) src
UNPIVOT (
    amount FOR metric IN (distinct_ids, distinct_listener_ids, median_prior_rows_3d)
) unp
ORDER BY prior_rows_3d, metric

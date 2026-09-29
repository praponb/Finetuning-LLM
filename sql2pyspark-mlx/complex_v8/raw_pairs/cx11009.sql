WITH matched AS (
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
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        prior_rows_3d,
        prior_ms_played_3d,
        MAX(prior_ms_played_3d) OVER (PARTITION BY prior_rows_3d, YEAR(played_at) ORDER BY prior_rows_3d, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prior_ms_played_3d_max_6_prior,
        DENSE_RANK() OVER (PARTITION BY prior_rows_3d, YEAR(played_at) ORDER BY prior_rows_3d, id) AS prior_rows_3d_dense_rank,
        FIRST_VALUE(played_at) OVER (PARTITION BY prior_rows_3d, YEAR(played_at) ORDER BY prior_rows_3d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        COUNT(*) OVER (PARTITION BY listener_id, MONTH(played_at)) AS rows_in_listener_id_played_at_month
    FROM matched
)
-- Final projection
SELECT
    id,
    prior_rows_3d,
    prior_ms_played_3d,
    prior_ms_played_3d_max_6_prior,
    prior_rows_3d_dense_rank,
    rows_in_listener_id_played_at_month
FROM ranked
ORDER BY id DESC
LIMIT 10

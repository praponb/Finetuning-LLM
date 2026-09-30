WITH in_window AS (
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
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_3d,
        l.interests AS listener_interests
    FROM in_window s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_parent_listener s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id IS NOT NULL
        )
      AND s.listener_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        prior_rows_3d,
        listener_interests,
        NTH_VALUE(prior_rows_3d, 2) OVER (ORDER BY prior_rows_3d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_3d,
        ROUND(PERCENT_RANK() OVER (ORDER BY prior_rows_3d DESC), 3) AS prior_rows_3d_percent_rank,
        ROUND(prior_rows_3d - AVG(prior_rows_3d) OVER (PARTITION BY prior_rows_3d, listener_id), 2) AS prior_rows_3d_vs_prior_rows_3d_listener_id_avg,
        SUM(prior_rows_3d) OVER (ORDER BY prior_rows_3d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prior_rows_3d,
        SUM(prior_rows_3d) OVER (ORDER BY prior_rows_3d DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prior_rows_3d_sum_3_prior
    FROM with_activity
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    prior_rows_3d,
    listener_interests,
    prior_rows_3d_percent_rank,
    prior_rows_3d_vs_prior_rows_3d_listener_id_avg,
    running_prior_rows_3d
FROM sequenced
ORDER BY played_at NULLS LAST

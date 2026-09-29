WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(prior_rows_7d) OVER (PARTITION BY prior_rows_7d ORDER BY prior_ms_played_7d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prior_rows_7d,
        MAX(prior_ms_played_7d) OVER w_listener_id AS listener_id_max_prior_ms_played_7d,
        MIN(prior_ms_played_7d) OVER (PARTITION BY listener_id) AS listener_id_min_prior_ms_played_7d,
        ROUND(CUME_DIST() OVER (PARTITION BY prior_rows_7d ORDER BY prior_ms_played_7d DESC), 3) AS prior_ms_played_7d_cume_dist,
        COUNT(*) OVER w_listener_id AS rows_in_listener_id
    FROM in_window
    WINDOW w_listener_id AS (PARTITION BY listener_id)
)
-- Subtotals with CUBE
SELECT
    listener_id,
    prior_rows_7d,
    SUM(prior_rows_7d) AS total_prior_rows_7d,
    MAX(played_at) AS last_played_at
FROM with_windows
GROUP BY CUBE(listener_id, prior_rows_7d)
ORDER BY listener_id NULLS LAST, prior_rows_7d NULLS LAST

WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.ms_played) AS prior_ms_played_12h
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(prior_ms_played_12h) OVER (ORDER BY prior_rows_12h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_ms_played_12h,
        RANK() OVER w_all_by_prior_rows_12h AS prior_rows_12h_rank,
        ROUND(AVG(prior_rows_12h) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_prior_rows_12h,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY prior_rows_12h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at
    FROM matched
    WINDOW w_all_by_prior_rows_12h AS (ORDER BY prior_rows_12h DESC, id)
)
SELECT
    id,
    listener_id,
    played_at,
    prior_rows_12h,
    prior_ms_played_12h,
    first_prior_ms_played_12h,
    prior_rows_12h_rank,
    listener_id_avg_prior_rows_12h,
    ROUND(prior_ms_played_12h / NULLIF(prior_rows_12h_rank, 0), 3) AS prior_ms_played_12h_per_prior_rows_12h_rank
FROM with_windows
ORDER BY prior_rows_12h DESC NULLS FIRST, listener_id

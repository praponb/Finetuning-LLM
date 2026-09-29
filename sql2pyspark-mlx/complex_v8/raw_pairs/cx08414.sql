WITH joined_ranges AS (
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
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        prior_rows_7d,
        LEAST(prior_rows_7d, prior_ms_played_7d) AS least_prior_rows_7d_prior_ms_played_7d,
        COALESCE(prior_ms_played_7d, 0) AS prior_ms_played_7d_filled
    FROM joined_ranges
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(prior_ms_played_7d_filled) OVER w_listener_id_by_prior_ms_play AS prev_prior_ms_played_7d_filled,
        COUNT(*) OVER (PARTITION BY listener_id, prior_rows_7d ORDER BY prior_ms_played_7d_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(prior_rows_7d, 2) OVER (PARTITION BY listener_id ORDER BY prior_ms_played_7d_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_7d,
        FIRST_VALUE(prior_rows_7d) OVER (PARTITION BY listener_id, prior_rows_7d ORDER BY prior_ms_played_7d_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_rows_7d,
        MAX(prior_ms_played_7d_filled) OVER (PARTITION BY listener_id ORDER BY prior_ms_played_7d_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prior_ms_played_7d_filled_so_far,
        prior_ms_played_7d_filled - LAG(prior_ms_played_7d_filled) OVER (PARTITION BY listener_id ORDER BY prior_ms_played_7d_filled DESC, id) AS prior_ms_played_7d_filled_change
    FROM tagged
    WINDOW w_listener_id_by_prior_ms_play AS (PARTITION BY listener_id ORDER BY prior_ms_played_7d_filled DESC, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(first_prior_rows_7d / NULLIF(prior_rows_7d, 0), 3) AS first_prior_rows_7d_per_prior_rows_7d,
        LEAST(running_count, second_prior_rows_7d) AS least_running_count_second_prior_rows_7d,
        ROUND(running_count / NULLIF(second_prior_rows_7d, 0), 3) AS running_count_per_second_prior_rows_7d
    FROM ranked
)
SELECT
    id,
    listener_id,
    prior_rows_7d,
    least_prior_rows_7d_prior_ms_played_7d,
    prior_ms_played_7d_filled,
    prev_prior_ms_played_7d_filled,
    running_count,
    second_prior_rows_7d,
    first_prior_rows_7d,
    max_prior_ms_played_7d_filled_so_far,
    prior_ms_played_7d_filled_change,
    least_running_count_second_prior_rows_7d,
    running_count_per_second_prior_rows_7d,
    ROUND(first_prior_rows_7d / NULLIF(running_count, 0), 3) AS first_prior_rows_7d_per_running_count
FROM derived
WHERE max_prior_ms_played_7d_filled_so_far >= 5000000

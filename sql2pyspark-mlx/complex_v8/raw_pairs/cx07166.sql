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
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        prior_rows_7d,
        MAX(prior_rows_7d) OVER (ORDER BY prior_ms_played_7d ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prior_rows_7d_max_2,
        ROW_NUMBER() OVER (ORDER BY prior_ms_played_7d ASC NULLS LAST, id) AS seq_no,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY prior_ms_played_7d ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        SUM(prior_ms_played_7d) OVER w_listener_id_prior_rows_7d AS listener_id_prior_rows_7d_sum_prior_ms_played_7d,
        NTILE(10) OVER (ORDER BY prior_ms_played_7d ASC NULLS LAST, id) AS prior_ms_played_7d_decile
    FROM joined_ranges
    WINDOW w_listener_id_prior_rows_7d AS (PARTITION BY listener_id, prior_rows_7d)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        prior_ms_played_7d_decile,
        MAX(prior_ms_played_7d_decile) AS max_prior_ms_played_7d_decile,
        ROUND(SUM(prior_rows_7d_max_2) / NULLIF(SUM(listener_id_prior_rows_7d_sum_prior_ms_played_7d), 0), 3) AS prior_rows_7d_max_2_to_listener_id_prior_rows_7d,
        MIN(played_at) AS first_played_at,
        ROUND(STDDEV(prior_rows_7d), 2) AS stddev_prior_rows_7d,
        ROUND(AVG(prior_rows_7d), 2) AS avg_prior_rows_7d,
        SUM(prior_ms_played_7d_decile) AS total_prior_ms_played_7d_decile
    FROM with_windows
    GROUP BY seq_no, prior_ms_played_7d_decile
    HAVING MAX(prior_rows_7d_max_2) > 4
)
-- Final projection
SELECT
    seq_no,
    prior_ms_played_7d_decile,
    avg_prior_rows_7d,
    total_prior_ms_played_7d_decile,
    DAYOFWEEK(first_played_at) AS first_played_at_dayofweek
FROM per_group
ORDER BY total_prior_ms_played_7d_decile DESC

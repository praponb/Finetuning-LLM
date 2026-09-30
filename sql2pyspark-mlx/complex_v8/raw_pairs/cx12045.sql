WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MIN(played_at) AS first_played_at,
        COUNT(*) AS play_count,
        COUNT(*) AS play_count_2
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM in_window s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
latest AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        played_at,
        prior_rows_1d,
        prior_ms_played_1d,
        first_played_at,
        play_count,
        play_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Group-level totals
    SELECT
        prior_rows_1d,
        play_count,
        COUNT(*) AS row_count,
        MIN(play_count) AS min_play_count,
        MIN(played_at) AS first_played_at_2,
        MIN(first_played_at) AS first_first_played_at,
        MAX(first_played_at) AS last_first_played_at,
        ROUND(SUM(prior_rows_1d) / NULLIF(SUM(prior_ms_played_1d), 0), 3) AS prior_rows_1d_to_prior_ms_played_1d_ratio
    FROM latest
    WHERE play_count_2 <= 5
    GROUP BY prior_rows_1d, play_count
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    prior_rows_1d,
    play_count,
    min_play_count,
    first_played_at_2,
    first_first_played_at,
    last_first_played_at,
    DATE_TRUNC('QUARTER', last_first_played_at) AS last_first_played_at_quarter
FROM totals
ORDER BY min_play_count

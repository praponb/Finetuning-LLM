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
filtered AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND UPPER(TRIM(l.username)) LIKE 'D%'
              AND l.plan <> 'free'
        )
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MIN(ms_played) AS min_play_ms_played,
        COUNT(*) AS play_count,
        COUNT(*) AS play_count_2
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM filtered s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        ABS(prior_rows_3d - 5) AS prior_rows_3d_distance
    FROM profiled
    WHERE played_at < TIMESTAMP '2025-02-24 09:00:00'
)
SELECT
    play_count,
    prior_rows_3d_distance,
    listener_id,
    MAX(played_at) AS last_played_at,
    COUNT(*) AS row_count,
    MIN(played_at) AS first_played_at
FROM scored
GROUP BY GROUPING SETS ((play_count, prior_rows_3d_distance, listener_id), (play_count), (prior_rows_3d_distance), ())
ORDER BY play_count NULLS LAST, prior_rows_3d_distance NULLS LAST, listener_id NULLS LAST

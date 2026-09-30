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
combined_details_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_3d,
        l.plan AS listener_plan
    FROM in_window s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.plan = 'free'
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM combined_details_listener s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND NOT p.is_skipped
        )
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.listener_plan,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.track_id AS play_track_id,
        p.device AS play_device
    FROM with_activity s
    JOIN plays p ON p.listener_id = s.listener_id AND p.device IS NOT NULL
)
SELECT
    play_track_id,
    play_is_skipped,
    play_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows,
    ARRAY_SORT(COLLECT_SET(listener_plan)) AS listener_plan_list,
    COUNT(DISTINCT play_device) AS distinct_play_devices
FROM detail_rows_plays
GROUP BY CUBE(play_track_id, play_is_skipped, play_id)
ORDER BY play_track_id NULLS LAST, play_is_skipped NULLS LAST, play_id NULLS LAST

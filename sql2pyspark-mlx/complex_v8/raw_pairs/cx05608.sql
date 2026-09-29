WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CASE WHEN device IN ('tv', 'desktop') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE track_id <= 3
      AND MONTH(played_at) IN (1, 3)
),
enriched AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.device_group,
        s.days_since_played_at,
        s.source_clean,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit
    FROM scoped_plays s
    JOIN tracks t ON t.id = s.track_id
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.device_group,
        s.days_since_played_at,
        s.source_clean,
        s.track_release_date,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at
    FROM enriched s
    JOIN plays p ON p.track_id = s.track_id
),
enriched_play AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.device_group,
        s.days_since_played_at,
        s.source_clean,
        s.track_release_date,
        s.play_id,
        s.play_is_skipped,
        s.play_listener_id,
        s.play_played_at,
        p.device AS play_device,
        p.played_at AS play_played_at_2,
        p.track_id AS play_track_id,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date_2
    FROM with_children s
    LEFT JOIN plays p ON p.id = s.play_id
    LEFT JOIN tracks t ON t.id = p.track_id
)
SELECT
    play_track_id,
    track_id,
    COUNT(*) AS n_rows,
    ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list,
    MAX(track_release_date_2) AS last_track_release_date_2
FROM enriched_play
GROUP BY GROUPING SETS ((play_track_id, track_id), (play_track_id), (track_id), ())
ORDER BY play_track_id NULLS LAST, track_id NULLS LAST

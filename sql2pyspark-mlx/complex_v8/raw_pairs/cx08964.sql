WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        LOWER(TRIM(device)) AS device_clean,
        INITCAP(TRIM(source)) AS source_clean
    FROM plays
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        device_clean,
        source_clean,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY MONTH(played_at), listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        LAG(played_at) OVER (PARTITION BY MONTH(played_at), listener_id ORDER BY played_at, id) AS prev_played_at,
        LEAD(played_at) OVER w_played_at_month_listener_id_2 AS next_played_at
    FROM cleaned_plays
    WINDOW w_played_at_month_listener_id_2 AS (PARTITION BY MONTH(played_at), listener_id ORDER BY played_at, id)
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.track_id,
        s.is_skipped,
        s.source_clean,
        s.next_played_at,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM windowed s
    LEFT JOIN plays p ON p.track_id = s.track_id
)
SELECT
    id,
    track_id,
    is_skipped,
    source_clean,
    next_played_at,
    play_id,
    play_played_at,
    play_ms_played,
    CONCAT(source_clean, '#', CAST(id AS STRING)) AS source_clean_ref
FROM with_children_plays
ORDER BY source_clean_ref DESC, source_clean DESC NULLS FIRST

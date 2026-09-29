WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE bpm BETWEEN 85 AND 143
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        moods,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY is_explicit ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        LEAD(bpm) OVER w_is_explicit_by_bpm AS next_bpm,
        LEAD(duration_sec, 1, 0) OVER w_all_by_release_date AS next_duration_sec
    FROM prepared
    WINDOW w_is_explicit_by_bpm AS (PARTITION BY is_explicit ORDER BY bpm NULLS LAST, id), w_all_by_release_date AS (ORDER BY release_date DESC, id)
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.second_release_date,
        s.next_bpm,
        s.next_duration_sec,
        t.mood
    FROM windowed s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT COUNT(*) FILTER (WHERE p.source = 'radio') FROM plays p WHERE p.track_id = s.id) AS play_source_count
    FROM flattened s
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM summarized_rows s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.is_skipped = TRUE
              AND p.listener_id IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    release_date,
    next_duration_sec,
    mood,
    last_played_at,
    play_source_count,
    ROUND(play_source_count / NULLIF(duration_sec, 0), 3) AS play_source_count_per_duration_sec,
    TO_DATE(last_played_at) AS last_played_at_date
FROM screened
ORDER BY title DESC NULLS FIRST, last_played_at DESC NULLS FIRST

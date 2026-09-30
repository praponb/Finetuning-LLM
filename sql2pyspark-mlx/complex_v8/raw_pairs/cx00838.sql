WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN duration_sec >= 357 THEN 'A' WHEN duration_sec >= 164 THEN 'B' ELSE 'C' END AS duration_sec_tier
    FROM tracks
    WHERE release_date < DATE '2025-03-08'
      AND artist_id IS NOT NULL
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.duration_sec_tier,
        t.mood
    FROM filtered s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.mood,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped
    FROM exploded s
    JOIN plays p ON p.track_id = s.id AND p.is_skipped = FALSE
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.mood,
        s.play_id,
        s.play_ms_played,
        s.play_source,
        s.play_listener_id,
        p.id AS play_id_2,
        p.device AS play_device,
        p.ms_played AS play_ms_played_2,
        p.played_at AS play_played_at
    FROM detail_rows_plays s
    JOIN plays p ON p.track_id = s.id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, play_listener_id ORDER BY duration_sec, id, mood, play_id, play_id_2) AS seq_no,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY YEAR(play_played_at), play_source ORDER BY release_date DESC, id, mood, play_id, play_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date
    FROM line_level_plays
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        mood,
        TO_DATE(play_played_at) AS play_played_at_day,
        COUNT(DISTINCT play_source) AS distinct_play_sources,
        COUNT(DISTINCT play_device) AS distinct_play_devices,
        ROUND(AVG(play_ms_played_2), 2) AS avg_play_ms_played_2,
        ROUND(STDDEV(play_ms_played), 2) AS stddev_play_ms_played,
        MAX(seq_no) AS max_seq_no
    FROM windowed
    GROUP BY 1, 2
)
SELECT
    mood,
    play_played_at_day,
    distinct_play_sources,
    distinct_play_devices,
    avg_play_ms_played_2,
    stddev_play_ms_played,
    max_seq_no,
    CASE WHEN mood IN ('sad', 'party') THEN 'group_a' WHEN mood = 'workout' THEN 'group_b' ELSE 'other' END AS mood_group,
    LOWER(REGEXP_REPLACE(TRIM(mood), ' ', '_')) AS mood_slug
FROM per_group
WHERE avg_play_ms_played_2 >= 84000.0
ORDER BY distinct_play_sources DESC NULLS FIRST, max_seq_no NULLS LAST, mood, play_played_at_day
LIMIT 10

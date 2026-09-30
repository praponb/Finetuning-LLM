WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE SIZE(moods) >= 2
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM base_tracks s
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(bpm) OVER (PARTITION BY play_count, artist_id), 2) AS play_count_artist_id_avg_bpm,
        NTILE(3) OVER (PARTITION BY artist_id, play_count ORDER BY bpm DESC, id) AS bpm_tercile,
        LAST_VALUE(play_count) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_play_count,
        MIN(play_count) OVER w_play_count_artist_id AS play_count_artist_id_min_play_count
    FROM profiled
    WINDOW w_play_count_artist_id AS (PARTITION BY play_count, artist_id)
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        play_count,
        play_count_artist_id_avg_bpm,
        bpm_tercile,
        last_known_play_count,
        play_count_artist_id_min_play_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY play_count_artist_id_min_play_count DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
artist_id_stats AS (
    -- Avg play_count per artist_id
    SELECT artist_id, AVG(play_count) AS avg_play_count
    FROM deduped
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_play_count,
        ROUND(s.play_count - g.avg_play_count, 2) AS play_count_minus_avg
    FROM deduped s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.play_count > g.avg_play_count
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        play_count_artist_id_avg_bpm,
        last_known_play_count,
        avg_play_count,
        play_count_minus_avg,
        ROUND(AVG(play_count_artist_id_avg_bpm) OVER (PARTITION BY play_count, is_explicit ORDER BY play_count ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS play_count_artist_id_avg_bpm_avg_4,
        FIRST_VALUE(play_count_minus_avg) IGNORE NULLS OVER (PARTITION BY play_count, is_explicit ORDER BY play_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_play_count_minus_avg
    FROM compared
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    moods,
    play_count_artist_id_avg_bpm,
    last_known_play_count,
    avg_play_count,
    play_count_minus_avg,
    play_count_artist_id_avg_bpm_avg_4,
    first_known_play_count_minus_avg,
    GREATEST(duration_sec, avg_play_count) AS greatest_duration_sec_avg_play_count,
    COALESCE(last_known_play_count, 0) AS last_known_play_count_filled
FROM windowed
ORDER BY title NULLS LAST, artist_id

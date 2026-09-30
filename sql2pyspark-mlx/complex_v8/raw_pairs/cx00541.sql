WITH filtered AS (
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
    WHERE (is_explicit = FALSE OR LOWER(title) LIKE '%so%')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(bpm, 2) OVER (ORDER BY bpm, id) AS prev2_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_is_explicit_artist_id, 0), 4) AS bpm_share_of_is_explicit_artist_id,
        MAX(duration_sec) OVER (PARTITION BY is_explicit ORDER BY bpm ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_max_7
    FROM filtered
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
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
        prev2_bpm,
        bpm_share_of_is_explicit_artist_id,
        duration_sec_max_7
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY prev2_bpm DESC, id DESC) AS row_num
        FROM windowed
    ) ranked
    WHERE row_num = 1
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM deduped
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM deduped s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
artist_id_stats_2 AS (
    -- Avg bpm_share_of_is_explicit_artist_id per artist_id
    SELECT artist_id, AVG(bpm_share_of_is_explicit_artist_id) AS avg_bpm_share_of_is_explicit_artist_id
    FROM above_average
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_share_of_is_explicit_artist_id,
        ROUND(s.bpm_share_of_is_explicit_artist_id - g.avg_bpm_share_of_is_explicit_artist_id, 2) AS bpm_share_of_is_explicit_artist_id_minus_avg
    FROM above_average s
    JOIN artist_id_stats_2 g ON g.artist_id = s.artist_id
    WHERE s.bpm_share_of_is_explicit_artist_id > g.avg_bpm_share_of_is_explicit_artist_id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        s.prev2_bpm,
        s.bpm_share_of_is_explicit_artist_id,
        s.duration_sec_max_7,
        s.bpm_minus_avg,
        s.avg_bpm_share_of_is_explicit_artist_id,
        s.bpm_share_of_is_explicit_artist_id_minus_avg,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM compared s
    JOIN plays p ON p.track_id = s.id
)
SELECT
    id,
    artist_id,
    moods,
    bpm_share_of_is_explicit_artist_id,
    duration_sec_max_7,
    bpm_minus_avg,
    avg_bpm_share_of_is_explicit_artist_id,
    bpm_share_of_is_explicit_artist_id_minus_avg,
    play_id,
    play_played_at,
    play_device,
    play_is_skipped
FROM exploded_detail
WHERE play_id IS NOT NULL
ORDER BY duration_sec_max_7 NULLS LAST, bpm_share_of_is_explicit_artist_id_minus_avg DESC NULLS FIRST

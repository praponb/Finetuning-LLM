WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 90) AS release_date_plus_90d,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        release_date_plus_90d,
        days_since_release_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date_plus_90d DESC, id DESC) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM deduped
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM deduped s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.days_since_release_date,
        s.avg_bpm,
        s.bpm_minus_avg,
        a.artist_name
    FROM compared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count, COUNT(*) AS play_count_2
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM enriched_artist s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    days_since_release_date,
    avg_bpm,
    bpm_minus_avg,
    artist_name,
    play_count,
    play_count_2,
    CONCAT(artist_name, '#', CAST(days_since_release_date AS STRING)) AS artist_name_ref
FROM profiled
WHERE days_since_release_date < 525
ORDER BY id DESC NULLS FIRST, artist_name NULLS LAST

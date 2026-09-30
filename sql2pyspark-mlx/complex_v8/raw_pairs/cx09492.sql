WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE artist_id <= 5
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        is_explicit_flag,
        bpm_per_duration_sec,
        LAG(bpm, 3) OVER (PARTITION BY artist_id ORDER BY bpm_per_duration_sec DESC NULLS FIRST, id) AS prev3_bpm,
        ROUND(is_explicit_flag / NULLIF(SUM(is_explicit_flag) OVER (PARTITION BY is_explicit_flag), 0), 4) AS is_explicit_flag_share_of_is_explicit_flag
    FROM valid_tracks
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(CASE WHEN device IN ('car', 'tv') THEN 1 END) AS play_device_count,
        MIN(played_at) AS first_played_at,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_device_count, 0) AS play_device_count,
        p.first_played_at,
        p.avg_play_ms_played
    FROM ranked s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
enriched AS (
    -- Attach artist attributes
    SELECT s.*, a.artist_name, a.genre AS artist_genre
    FROM summarized_rows s
    JOIN artists a ON a.id = s.artist_id
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit_flag,
        s.bpm_per_duration_sec,
        s.prev3_bpm,
        s.is_explicit_flag_share_of_is_explicit_flag,
        s.first_played_at,
        s.avg_play_ms_played,
        s.artist_name,
        s.artist_genre,
        a.country AS artist_country,
        a.artist_name AS artist_artist_name,
        a.is_verified AS artist_is_verified
    FROM enriched s
    JOIN artists a ON a.id = s.artist_id
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.is_verified AS artist_is_verified_2,
        a.country AS artist_country_2,
        a.genre AS artist_genre_2
    FROM joined s
    JOIN artists a ON a.id = s.artist_id
)
SELECT id, metric, amount
FROM (
    SELECT id, prev3_bpm, is_explicit_flag
    FROM joined_artist
) src
UNPIVOT (
    amount FOR metric IN (prev3_bpm AS prev3_bpm_v, is_explicit_flag AS m_is_explicit_flag)
) unp
ORDER BY id, metric

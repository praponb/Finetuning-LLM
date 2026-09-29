WITH prepared_tracks AS (
    -- Filter tracks
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
    WHERE duration_sec <= 352
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        a.genre AS artist_genre
    FROM prepared_tracks s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, artist_genre), 0), 4) AS bpm_share_of_artist_id_artist_genre,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_artist_genre, 2) AS duration_sec_vs_artist_id_artist_genre_avg,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit, artist_genre)) AS distinct_artist_id_in_is_explicit_artist_genre,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_genre) AS rows_in_is_explicit_artist_genre,
        DENSE_RANK() OVER (PARTITION BY artist_genre ORDER BY release_date DESC) AS release_date_dense_rank,
        LEAD(duration_sec, 2) OVER w_artist_genre_by_release_date_2 AS next2_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY artist_id, artist_genre) AS artist_id_artist_genre_min_duration_sec,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_genre), 2) AS is_explicit_artist_genre_avg_duration_sec
    FROM with_parent
    WINDOW w_artist_id_artist_genre AS (PARTITION BY artist_id, artist_genre), w_is_explicit_artist_genre AS (PARTITION BY is_explicit, artist_genre), w_artist_genre_by_release_date_2 AS (PARTITION BY artist_genre ORDER BY release_date DESC, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(duration_sec / NULLIF(next2_duration_sec, 0), 3) AS duration_sec_per_next2_duration_sec
    FROM metrics
    WHERE duration_sec_vs_artist_id_artist_genre_avg < 168.0
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    bpm_share_of_artist_id_artist_genre,
    last_known_bpm,
    distinct_artist_id_in_is_explicit_artist_genre,
    release_date_dense_rank,
    next2_duration_sec,
    duration_sec_per_next2_duration_sec,
    CASE WHEN next2_duration_sec >= 352 THEN 'A' WHEN next2_duration_sec >= 184 THEN 'B' ELSE 'C' END AS next2_duration_sec_tier,
    COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
FROM derived
ORDER BY bpm_share_of_artist_id_artist_genre DESC NULLS FIRST, last_known_bpm NULLS LAST

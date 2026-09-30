WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ABS(bpm - 142) AS bpm_distance
    FROM tracks
    WHERE is_explicit = TRUE
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre NOT IN ('Pop', 'HipHop')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        artist_is_verified,
        artist_debut_year,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_is_verified), 2) AS artist_debut_year_vs_artist_is_verified_avg,
        MAX(artist_debut_year) OVER (PARTITION BY artist_id) AS artist_id_max_artist_debut_year,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY artist_is_verified)) AS distinct_artist_genre_in_artist_is_verified,
        ROUND(AVG(duration_sec) OVER w_artist_is_verified, 2) AS artist_is_verified_avg_duration_sec,
        MAX(artist_debut_year) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_max_artist_debut_year,
        LEAD(bpm, 1, 0) OVER w_all_by_bpm AS next_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        LEAD(duration_sec, 3) OVER (PARTITION BY artist_genre, artist_is_verified ORDER BY bpm, id) AS next3_duration_sec,
        SUM(bpm) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_sum_bpm,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
    FROM combined_details
    WINDOW w_artist_is_verified AS (PARTITION BY artist_is_verified), w_all_by_bpm AS (ORDER BY bpm, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    moods,
    artist_debut_year,
    distinct_artist_genre_in_artist_is_verified,
    artist_is_verified_max_artist_debut_year,
    next_bpm,
    bpm_vs_artist_id_avg,
    next3_duration_sec,
    artist_is_verified_sum_bpm,
    last_known_duration_sec,
    artist_debut_year - next3_duration_sec AS artist_debut_year_minus_next3_duration_sec,
    ROUND(bpm_vs_artist_id_avg * 0.25, 2) AS bpm_vs_artist_id_avg_adj
FROM with_windows
ORDER BY artist_debut_year, artist_is_verified_sum_bpm

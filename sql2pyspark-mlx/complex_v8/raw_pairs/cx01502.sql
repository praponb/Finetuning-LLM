WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        bpm + duration_sec AS bpm_plus_duration_sec,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        ARRAY_CONTAINS(moods, 'party') AS has_party
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.duration_sec_filled,
        s.has_party,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM cleaned_tracks s
    LEFT JOIN plays p ON p.track_id = s.id
),
with_details AS (
    -- Join artists
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM with_children s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre <> 'Classical'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(play_played_at) IGNORE NULLS OVER (PARTITION BY is_explicit, play_id ORDER BY duration_sec_filled, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_play_played_at,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY artist_debut_year ASC) AS artist_debut_year_dense_rank,
        FIRST_VALUE(play_played_at) OVER (PARTITION BY is_explicit, play_id ORDER BY duration_sec_filled, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_play_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY artist_debut_year ASC, id, play_id), 3) AS artist_debut_year_percent_rank,
        NTILE(10) OVER (PARTITION BY is_explicit ORDER BY artist_debut_year ASC, id, play_id) AS artist_debut_year_decile
    FROM with_details
)
-- Subtotals with GROUPING SETS
SELECT
    play_id,
    artist_debut_year_decile,
    play_is_skipped,
    GROUPING(play_id) AS is_total_play_id,
    COUNT(*) FILTER (WHERE UPPER(TRIM(title)) LIKE 'C%') AS n_upper_trim_title_like_c
FROM windowed
GROUP BY GROUPING SETS ((play_id, artist_debut_year_decile, play_is_skipped), (play_id), ())

WITH filtered_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(debut_year, 0) AS debut_year_filled,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        COALESCE(debut_year, 0) AS debut_year_filled_2
    FROM artists
    WHERE is_verified = TRUE
),
unique_rows AS (
    -- Keep the latest row per genre, country
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        debut_year_filled,
        country_or_unknown,
        debut_year_filled_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY genre, country ORDER BY debut_year_filled_2 DESC, id DESC) AS recency_rank
        FROM filtered_artists
    ) ranked
    WHERE recency_rank = 1
),
detail_rows AS (
    -- One row per track
    SELECT
        s.id,
        s.genre,
        s.country,
        s.is_verified,
        s.debut_year_filled,
        t.id AS track_id,
        t.duration_sec AS track_duration_sec,
        t.is_explicit AS track_is_explicit,
        t.moods AS track_moods
    FROM unique_rows s
    JOIN tracks t ON t.artist_id = s.id
),
joined_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.genre,
        s.is_verified,
        s.debut_year_filled,
        s.track_id,
        s.track_is_explicit,
        s.track_moods,
        t.bpm AS track_bpm,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit_2,
        a.is_verified AS artist_is_verified
    FROM detail_rows s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        artist_is_verified,
        genre,
        is_verified,
        MIN(track_bpm) AS min_track_bpm,
        MAX(debut_year_filled) AS max_debut_year_filled,
        DATEDIFF(MAX(track_release_date), MIN(track_release_date)) AS track_release_date_span_days
    FROM joined_track
    WHERE NOT track_is_explicit
    GROUP BY artist_is_verified, genre, is_verified
    HAVING SUM(track_bpm) > 75
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM rollup_base
    WHERE min_track_bpm < (SELECT MAX(min_track_bpm) FROM rollup_base)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(min_track_bpm) OVER (ORDER BY min_track_bpm DESC, artist_is_verified, genre, is_verified ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_track_bpm_sum_2,
        ROUND(max_debut_year_filled - AVG(max_debut_year_filled) OVER (PARTITION BY genre, is_verified), 2) AS max_debut_year_filled_vs_genre_is_verified_avg
    FROM with_activity
)
-- Final projection
SELECT
    artist_is_verified,
    is_verified,
    min_track_bpm,
    max_debut_year_filled,
    track_release_date_span_days,
    min_track_bpm_sum_2,
    max_debut_year_filled_vs_genre_is_verified_avg,
    FLOOR(min_track_bpm / 25) * 25 AS min_track_bpm_bucket
FROM metrics

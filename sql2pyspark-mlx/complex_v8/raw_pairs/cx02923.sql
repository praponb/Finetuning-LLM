WITH base_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN debut_year >= 2018 THEN 'A' WHEN debut_year >= 1996 THEN 'B' ELSE 'C' END AS debut_year_tier,
        ABS(debut_year - 1997) AS debut_year_distance
    FROM artists
    WHERE country NOT IN ('KR', 'BR')
      AND genre = 'HipHop'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        debut_year_distance,
        ROUND(AVG(debut_year) OVER (PARTITION BY genre, debut_year_tier), 2) AS genre_debut_year_tier_avg_debut_year,
        MIN(debut_year_distance) OVER w_genre_debut_year_tier AS genre_debut_year_tier_min_debut_year_distance,
        NTH_VALUE(debut_year_distance, 2) OVER (PARTITION BY is_verified ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_distance,
        DENSE_RANK() OVER (PARTITION BY is_verified ORDER BY debut_year, id) AS debut_year_dense_rank
    FROM base_artists
    WINDOW w_genre_debut_year_tier AS (PARTITION BY genre, debut_year_tier)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        debut_year_distance,
        genre_debut_year_tier_avg_debut_year,
        genre_debut_year_tier_min_debut_year_distance,
        second_debut_year_distance,
        debut_year_dense_rank,
        ABS(debut_year_distance - 809) AS debut_year_distance_distance,
        ROUND(debut_year_distance / NULLIF(debut_year, 0), 3) AS debut_year_distance_per_debut_year
    FROM ranked
    WHERE genre_debut_year_tier_min_debut_year_distance <= 12138
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT SUM(t.bpm) FROM tracks t WHERE t.artist_id = s.id) AS total_track_bpm
    FROM derived s
)
SELECT
    id,
    debut_year_distance,
    genre_debut_year_tier_avg_debut_year,
    genre_debut_year_tier_min_debut_year_distance,
    second_debut_year_distance,
    debut_year_distance_per_debut_year,
    total_track_bpm,
    COALESCE(total_track_bpm, 0) AS total_track_bpm_filled,
    debut_year_distance + total_track_bpm AS debut_year_distance_plus_total_track_bpm
FROM summarized_rows

WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 5
      AND NOT ARRAY_CONTAINS(moods, 'sad')
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm,
        a.debut_year AS artist_debut_year
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(artist_debut_year) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_min_artist_debut_year,
        NTH_VALUE(bpm, 2) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        COUNT(*) OVER (ORDER BY artist_debut_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_sum_bpm,
        FIRST_VALUE(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER w_is_explicit_artist_id, 2) AS is_explicit_artist_id_avg_artist_debut_year,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY is_explicit, artist_id), 2) AS artist_debut_year_vs_is_explicit_artist_id_avg,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        MAX(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_max_bpm,
        RANK() OVER (ORDER BY artist_debut_year DESC, id) AS artist_debut_year_rank,
        ROUND(AVG(artist_debut_year) OVER (ORDER BY bpm, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS artist_debut_year_avg_7,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS bpm_share_of_is_explicit_artist_id,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER w_is_explicit_artist_id, 0), 4) AS artist_debut_year_share_of_is_explicit_artist_id
    FROM joined_artist
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(artist_debut_year_avg_7 - AVG(artist_debut_year_avg_7) OVER (PARTITION BY is_explicit, artist_debut_year_rank), 2) AS artist_debut_year_avg_7_vs_is_explicit_artist_de,
        ROW_NUMBER() OVER (PARTITION BY artist_id, artist_debut_year_rank ORDER BY is_explicit_artist_id_max_bpm DESC, id) AS seq_no,
        MIN(artist_debut_year_avg_7) OVER (PARTITION BY artist_id, artist_debut_year_rank ORDER BY is_explicit_artist_id_max_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_artist_debut_year_avg_7_so_far,
        MAX(running_count) OVER (PARTITION BY is_explicit) AS is_explicit_max_running_count
    FROM sequenced
)
-- Final projection
SELECT
    id,
    title,
    is_explicit,
    bpm,
    artist_debut_year,
    running_count,
    is_explicit_artist_id_sum_bpm,
    first_artist_debut_year,
    is_explicit_artist_id_avg_artist_debut_year,
    artist_debut_year_vs_is_explicit_artist_id_avg,
    is_explicit_artist_id_max_bpm,
    artist_debut_year_rank,
    artist_debut_year_avg_7,
    artist_debut_year_avg_7_vs_is_explicit_artist_de,
    seq_no,
    min_artist_debut_year_avg_7_so_far,
    is_explicit_max_running_count,
    ABS(artist_debut_year - 2015) AS artist_debut_year_distance,
    COALESCE(running_count, 0) AS running_count_filled
FROM with_trends
ORDER BY is_explicit_artist_id_sum_bpm

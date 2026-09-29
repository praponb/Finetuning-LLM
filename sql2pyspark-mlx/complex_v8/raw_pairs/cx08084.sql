WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(bpm - 118) AS bpm_distance,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND duration_sec BETWEEN 165 AND 352
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.bpm_distance,
        s.title_ref,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        bpm_distance,
        title_ref,
        artist_is_verified,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY is_explicit ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        RANK() OVER (ORDER BY release_date, id) AS release_date_rank,
        LEAD(artist_debut_year, 2) OVER w_all_by_bpm_distance AS next2_artist_debut_year,
        ROUND(bpm_distance / NULLIF(SUM(bpm_distance) OVER (PARTITION BY artist_country, is_explicit), 0), 4) AS bpm_distance_share_of_artist_country_is_explicit,
        SUM(bpm_distance) OVER (ORDER BY bpm_distance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS bpm_distance_sum_2_prior,
        COUNT(*) OVER w_artist_id_is_explicit AS rows_in_artist_id_is_explicit,
        DENSE_RANK() OVER (PARTITION BY is_explicit, artist_country ORDER BY release_date ASC) AS release_date_dense_rank,
        MAX(bpm_distance) OVER (ORDER BY bpm_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_distance_so_far,
        LAST_VALUE(bpm_distance) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm_distance
    FROM with_details
    WINDOW w_all_by_bpm_distance AS (ORDER BY bpm_distance, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        bpm_distance,
        title_ref,
        second_artist_debut_year,
        release_date_rank,
        bpm_distance_share_of_artist_country_is_explicit,
        bpm_distance_sum_2_prior,
        rows_in_artist_id_is_explicit,
        release_date_dense_rank,
        max_bpm_distance_so_far,
        last_known_bpm_distance,
        CONCAT(title_ref, '#', CAST(next2_artist_debut_year AS STRING)) AS title_ref_ref,
        SUBSTRING(TRIM(title_ref), 1, 3) AS title_ref_prefix
    FROM with_trends
    WHERE second_artist_debut_year BETWEEN 1996 AND 2020
)
SELECT
    id,
    artist_id,
    is_explicit,
    moods,
    bpm_distance,
    second_artist_debut_year,
    release_date_rank,
    bpm_distance_share_of_artist_country_is_explicit,
    bpm_distance_sum_2_prior,
    rows_in_artist_id_is_explicit,
    release_date_dense_rank,
    last_known_bpm_distance,
    title_ref_ref,
    title_ref_prefix,
    FLOOR(second_artist_debut_year / 5) * 5 AS second_artist_debut_year_bucket
FROM labeled
WHERE SIZE(moods) >= 2
ORDER BY rows_in_artist_id_is_explicit, release_date_rank

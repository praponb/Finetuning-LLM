WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(bpm - 118) AS bpm_distance,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_distance,
        s.duration_sec_filled,
        a.artist_name,
        a.genre AS artist_genre
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(duration_sec_filled, 2) OVER (PARTITION BY artist_genre, is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_filled,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec_filled ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec_filled ASC, id), 3) AS duration_sec_filled_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date DESC), 3) AS release_date_percent_rank,
        MAX(duration_sec_filled) OVER (PARTITION BY artist_genre) AS artist_genre_max_duration_sec_filled,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec_filled ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_2_prior,
        LEAD(duration_sec_filled) OVER w_all_by_release_date AS next_duration_sec_filled,
        LAST_VALUE(duration_sec_filled) IGNORE NULLS OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_filled,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_genre, is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        COUNT(*) OVER w_artist_genre AS rows_in_artist_genre,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm_2
    FROM joined_artist
    WINDOW w_all_by_release_date AS (ORDER BY release_date DESC, id), w_artist_genre AS (PARTITION BY artist_genre)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        moods,
        bpm_distance,
        duration_sec_filled,
        artist_genre,
        second_duration_sec_filled,
        bpm_avg_4,
        last_known_duration_sec_filled,
        first_bpm_2,
        LOWER(REGEXP_REPLACE(TRIM(artist_genre), ' ', '_')) AS artist_genre_slug,
        ARRAY_CONTAINS(moods, 'party') AS has_party,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM with_windows
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(first_bpm_2) OVER (PARTITION BY is_explicit ORDER BY bpm_avg_4 DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS first_bpm_2_sum_3_prior,
        SUM(second_duration_sec_filled) OVER (PARTITION BY artist_genre, is_explicit) AS artist_genre_is_explicit_sum_second_duration_sec,
        SUM(first_bpm_2) OVER (PARTITION BY artist_genre ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS first_bpm_2_sum_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_genre ORDER BY release_date), 3) AS release_date_percent_rank,
        SUM(bpm_distance) OVER (PARTITION BY artist_genre, is_explicit) AS artist_genre_is_explicit_sum_bpm_distance,
        LEAD(second_duration_sec_filled) OVER (PARTITION BY is_explicit ORDER BY bpm_avg_4 DESC, id) AS next_second_duration_sec_filled,
        SUM(duration_sec_filled) OVER (PARTITION BY artist_genre ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_filled_sum_2_prior
    FROM derived
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        artist_genre,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count,
        SUM(first_bpm_2_sum_3_prior) FILTER (WHERE is_explicit = FALSE) AS first_bpm_2_sum_3_prior_is_explicit_false
    FROM sequenced
    GROUP BY 1, 2
    HAVING MAX(bpm_avg_4) > 84.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY distinct_ids ORDER BY first_bpm_2_sum_3_prior_is_explicit_false DESC) AS first_bpm_2_sum_3_prior_is_explicit_false_dense_,
        NTILE(4) OVER w_distinct_ids_by_first_bpm_2_2 AS first_bpm_2_sum_3_prior_is_explicit_false_quarti
    FROM totals
    WINDOW w_distinct_ids_by_first_bpm_2_2 AS (PARTITION BY distinct_ids ORDER BY first_bpm_2_sum_3_prior_is_explicit_false DESC, release_date_ym, artist_genre)
)
SELECT
    release_date_ym,
    artist_genre,
    distinct_ids,
    row_count,
    first_bpm_2_sum_3_prior_is_explicit_false,
    first_bpm_2_sum_3_prior_is_explicit_false_dense_,
    first_bpm_2_sum_3_prior_is_explicit_false_quarti
FROM ranked
ORDER BY first_bpm_2_sum_3_prior_is_explicit_false_dense_ DESC NULLS FIRST

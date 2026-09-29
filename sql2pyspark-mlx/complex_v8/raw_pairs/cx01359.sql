WITH scoped AS (
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
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        ABS(bpm - 94) AS bpm_distance
    FROM tracks
    WHERE bpm BETWEEN 76 AND 155
      AND release_date >= DATE '2024-10-23'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_prefix,
        s.bpm_distance,
        a.artist_name,
        a.country AS artist_country
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(bpm, 1, 0) OVER (ORDER BY duration_sec ASC, id) AS prev_bpm,
        COUNT(*) OVER (PARTITION BY artist_country, is_explicit) AS rows_in_artist_country_is_explicit,
        ROUND(PERCENT_RANK() OVER w_is_explicit_by_bpm, 3) AS bpm_percent_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_country, 0), 4) AS bpm_share_of_artist_country,
        MAX(bpm) OVER (ORDER BY bpm_distance DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_max_3_prior,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY bpm_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit ORDER BY bpm_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        ROUND(AVG(bpm_distance) OVER w_is_explicit_artist_country, 2) AS is_explicit_artist_country_avg_bpm_distance,
        ROUND(AVG(bpm_distance) OVER (ORDER BY bpm_distance NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_distance_avg_7
    FROM combined_details
    WINDOW w_is_explicit_by_bpm AS (PARTITION BY is_explicit ORDER BY bpm DESC, id), w_artist_country AS (PARTITION BY artist_country), w_is_explicit_artist_country AS (PARTITION BY is_explicit, artist_country)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER w_artist_country_is_explicit_b AS bpm_max_3_prior_quartile,
        NTILE(3) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC, id) AS duration_sec_tercile,
        duration_sec - LAG(duration_sec) OVER w_all_by_prev_bpm AS duration_sec_change,
        NTILE(3) OVER (PARTITION BY artist_country, is_explicit ORDER BY bpm_max_3_prior DESC NULLS FIRST, id) AS bpm_max_3_prior_tercile,
        ROUND(first_known_duration_sec - AVG(first_known_duration_sec) OVER (PARTITION BY artist_id, artist_country), 2) AS first_known_duration_sec_vs_artist_id_artist_cou,
        COUNT(*) OVER (PARTITION BY artist_id, artist_country) AS rows_in_artist_id_artist_country
    FROM ranked
    WINDOW w_artist_country_is_explicit_b AS (PARTITION BY artist_country, is_explicit ORDER BY bpm_max_3_prior DESC NULLS FIRST, id), w_all_by_prev_bpm AS (ORDER BY prev_bpm ASC NULLS LAST, id)
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    title_prefix,
    bpm_distance,
    artist_name,
    artist_country,
    prev_bpm,
    rows_in_artist_country_is_explicit,
    bpm_percent_rank,
    first_known_duration_sec,
    second_duration_sec,
    bpm_distance_avg_7,
    bpm_max_3_prior_quartile,
    duration_sec_tercile,
    duration_sec_change,
    bpm_max_3_prior_tercile,
    ROUND(bpm_distance_avg_7 * 1.1, 2) AS bpm_distance_avg_7_adj,
    SUBSTRING(TRIM(artist_name), 1, 2) AS artist_name_prefix
FROM with_trends
WHERE title_prefix IS NOT NULL
ORDER BY second_duration_sec, bpm_distance_avg_7 NULLS LAST

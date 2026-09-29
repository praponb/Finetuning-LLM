WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        ABS(bpm - 104) AS bpm_distance
    FROM tracks
    WHERE SIZE(moods) >= 1
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.days_since_release_date,
        s.bpm_distance,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(bpm_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY bpm_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_distance,
        ROUND(AVG(bpm_distance) OVER (ORDER BY bpm_distance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS bpm_distance_avg_6_prior,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY bpm_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(PERCENT_RANK() OVER w_all_by_bpm_distance, 3) AS bpm_distance_percent_rank,
        NTH_VALUE(artist_debut_year, 2) OVER (ORDER BY bpm_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        LEAD(days_since_release_date, 1, 0) OVER (ORDER BY bpm_distance, id) AS next_days_since_release_date,
        ROUND(AVG(days_since_release_date) OVER (PARTITION BY artist_is_verified ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_release_date_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified ORDER BY release_date DESC), 3) AS release_date_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY bpm_distance, id), 3) AS bpm_distance_percent_rank_2,
        NTH_VALUE(release_date, 2) OVER (ORDER BY bpm_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_3
    FROM combined_details
    WINDOW w_all_by_bpm_distance AS (ORDER BY bpm_distance, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(bpm_distance_avg_6_prior / NULLIF(artist_debut_year, 0), 3) AS bpm_distance_avg_6_prior_per_artist_debut_year,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        CASE WHEN artist_country IN ('KR', 'BR') THEN 'group_a' WHEN artist_country = 'TH' THEN 'group_b' ELSE 'other' END AS artist_country_group
    FROM windowed
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(second_release_date) IGNORE NULLS OVER (PARTITION BY artist_country_group ORDER BY bpm_distance_percent_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_release_date,
        NTILE(10) OVER w_artist_is_verified_artist_co AS bpm_distance_avg_6_prior_per_artist_debut_year_d,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY artist_country_group ORDER BY bpm_distance_percent_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        SIZE(COLLECT_SET(artist_country_group) OVER w_artist_country_artist_id) AS distinct_artist_country_group_in_artist_country_,
        SUM(artist_debut_year) OVER (PARTITION BY artist_is_verified, artist_country ORDER BY bpm_distance_avg_6_prior_per_artist_debut_year, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS artist_debut_year_sum_3,
        ROUND(PERCENT_RANK() OVER w_all_by_bpm_distance_avg_6_pr, 3) AS bpm_distance_avg_6_prior_per_artist_debut_year_p
    FROM tagged
    WINDOW w_artist_is_verified_artist_co AS (PARTITION BY artist_is_verified, artist_country ORDER BY bpm_distance_avg_6_prior_per_artist_debut_year, id), w_artist_country_artist_id AS (PARTITION BY artist_country, artist_id), w_all_by_bpm_distance_avg_6_pr AS (ORDER BY bpm_distance_avg_6_prior_per_artist_debut_year ASC)
)
-- Final projection
SELECT
    id,
    artist_id,
    moods,
    days_since_release_date,
    bpm_distance,
    artist_is_verified,
    last_bpm_distance,
    bpm_distance_avg_6_prior,
    second_release_date,
    bpm_distance_percent_rank,
    second_artist_debut_year,
    next_days_since_release_date,
    release_date_percent_rank,
    bpm_distance_percent_rank_2,
    second_release_date_3,
    bpm_distance_avg_6_prior_per_artist_debut_year,
    title_slug,
    artist_country_group,
    first_known_second_release_date,
    bpm_distance_avg_6_prior_per_artist_debut_year_d,
    last_known_artist_debut_year,
    artist_debut_year_sum_3,
    bpm_distance_avg_6_prior_per_artist_debut_year_p
FROM metrics
WHERE release_date >= DATE '2024-10-23'
ORDER BY id NULLS LAST, last_known_artist_debut_year NULLS LAST

WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        CASE WHEN bpm >= 156 THEN 'large' WHEN bpm >= 76 THEN 'medium' ELSE 'small' END AS bpm_tier
    FROM tracks
    WHERE artist_id IS NOT NULL
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(release_date_week) IGNORE NULLS OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY release_date_week DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date_week,
        SUM(bpm) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY release_date_week DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_sum_6_prior,
        artist_debut_year - LAG(artist_debut_year) OVER (ORDER BY artist_debut_year, id) AS artist_debut_year_change,
        COUNT(*) OVER w_release_date_week_month_arti AS rows_in_release_date_week_month_artist_id,
        SUM(bpm) OVER (PARTITION BY bpm_tier, is_explicit ORDER BY release_date_week DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_sum_6_prior_2,
        MAX(artist_debut_year) OVER (PARTITION BY TO_DATE(release_date_week), bpm_tier) AS release_date_week_to_date_bpm_tier_max_artist_de,
        LAST_VALUE(artist_debut_year) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY release_date_week DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year,
        MAX(duration_sec) OVER (ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        FIRST_VALUE(release_date_week) IGNORE NULLS OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY release_date_week DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_week,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec), 3) AS duration_sec_percent_rank
    FROM with_details_artist
    WINDOW w_release_date_week_month_arti AS (PARTITION BY MONTH(release_date_week), artist_id), w_all_by_duration_sec_ties AS (ORDER BY duration_sec)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(max_duration_sec_so_far) OVER (ORDER BY release_date_week, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS max_duration_sec_so_far_avg_4,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit_bpm_tier) AS distinct_artist_id_in_is_explicit_bpm_tier,
        LAST_VALUE(bpm_sum_6_prior_2) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_sum_6_prior_2,
        ROUND((UNIX_TIMESTAMP(last_known_release_date_week) - UNIX_TIMESTAMP(LAG(last_known_release_date_week) OVER w_all_by_first_known_release_d)) / 60, 1) AS minutes_since_prev_last_known_release_date_week,
        bpm - LAG(bpm) OVER w_all_by_first_known_release_d AS bpm_change,
        FIRST_VALUE(duration_sec) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(CUME_DIST() OVER w_all_by_release_date_week, 3) AS release_date_week_cume_dist
    FROM sequenced
    WINDOW w_is_explicit_bpm_tier AS (PARTITION BY is_explicit, bpm_tier), w_all_by_first_known_release_d AS (ORDER BY first_known_release_date_week, id), w_all_by_release_date_week AS (ORDER BY release_date_week, id)
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    release_date_week,
    artist_is_verified,
    artist_debut_year,
    artist_name,
    last_known_release_date_week,
    artist_debut_year_change,
    rows_in_release_date_week_month_artist_id,
    last_artist_debut_year,
    max_duration_sec_so_far,
    first_known_release_date_week,
    duration_sec_percent_rank,
    max_duration_sec_so_far_avg_4,
    distinct_artist_id_in_is_explicit_bpm_tier,
    last_bpm_sum_6_prior_2,
    bpm_change,
    first_duration_sec,
    release_date_week_cume_dist
FROM with_trends
WHERE release_date < DATE '2024-12-22'
ORDER BY bpm DESC NULLS FIRST, duration_sec_percent_rank DESC

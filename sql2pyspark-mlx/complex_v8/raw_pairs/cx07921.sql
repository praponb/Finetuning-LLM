WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE is_explicit = TRUE
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.country AS artist_country,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = TRUE
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS bpm_vs_is_explicit_artist_id_avg,
        LAG(release_date_month) OVER (PARTITION BY is_explicit, artist_country ORDER BY release_date_month, id) AS prev_release_date_month,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        ROUND(CUME_DIST() OVER w_artist_country_by_release_da, 3) AS release_date_month_cume_dist,
        SUM(bpm) OVER (PARTITION BY artist_country ORDER BY release_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        SUM(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_sum_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY release_date_month, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS bpm_share_of_is_explicit_artist_id,
        ROUND((UNIX_TIMESTAMP(release_date_month) - UNIX_TIMESTAMP(LAG(release_date_month) OVER w_is_explicit_artist_country_b)) / 60, 1) AS minutes_since_prev_release_date_month,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_bpm
    FROM with_details
    WINDOW w_artist_country_by_release_da AS (PARTITION BY artist_country ORDER BY release_date_month ASC), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_is_explicit_artist_country_b AS (PARTITION BY is_explicit, artist_country ORDER BY release_date_month, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN artist_is_verified THEN 1 ELSE 0 END AS artist_is_verified_flag,
        COALESCE(running_bpm, 0) AS running_bpm_filled
    FROM metrics
    WHERE running_bpm >= 495
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(bpm) OVER (ORDER BY minutes_since_prev_release_date_month, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_1_prior,
        LAST_VALUE(prev_release_date_month) OVER (PARTITION BY is_explicit, TO_DATE(release_date_month) ORDER BY is_explicit_artist_id_avg_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_release_date_month,
        SUM(release_date_month_cume_dist) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_sum_release_date_month_cume_d,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_is_verified)) AS distinct_artist_country_in_artist_is_verified
    FROM derived
)
-- Final projection
SELECT
    id,
    artist_id,
    bpm,
    artist_country,
    artist_genre,
    artist_is_verified,
    bpm_vs_is_explicit_artist_id_avg,
    prev_release_date_month,
    rows_in_is_explicit_artist_id,
    release_date_month_cume_dist,
    running_bpm,
    is_explicit_artist_id_sum_bpm,
    bpm_avg_3_prior,
    bpm_share_of_is_explicit_artist_id,
    minutes_since_prev_release_date_month,
    is_explicit_artist_id_avg_bpm,
    artist_is_verified_flag,
    running_bpm_filled,
    bpm_avg_1_prior,
    last_prev_release_date_month,
    artist_is_verified_sum_release_date_month_cume_d,
    distinct_artist_country_in_artist_is_verified
FROM with_windows
ORDER BY running_bpm_filled DESC

WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE (bpm < 155 OR ARRAY_CONTAINS(moods, 'sad'))
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM recent s
    LEFT JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(duration_sec) OVER w_artist_id AS artist_id_min_duration_sec,
        LAST_VALUE(release_date_month) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date_month,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date), 3) AS release_date_cume_dist,
        MIN(days_since_release_date) OVER (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_release_date_so_far,
        MAX(bpm) OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bpm_max_7,
        MAX(duration_sec) OVER (ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_max_3,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC) AS days_since_release_date_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC, id) AS rn,
        SUM(days_since_release_date) OVER (PARTITION BY artist_id) AS artist_id_sum_days_since_release_date,
        NTILE(5) OVER (ORDER BY bpm, id) AS bpm_quintile,
        RANK() OVER w_artist_id_by_days_since_rele_3 AS days_since_release_date_rank,
        SUM(artist_debut_year) OVER (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS artist_debut_year_sum_3_prior,
        ROUND(CUME_DIST() OVER w_release_date_month_artist_is_4, 3) AS release_date_cume_dist_2,
        MIN(duration_sec) OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far
    FROM with_parent_artist
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_days_since_rele_3 AS (PARTITION BY artist_id ORDER BY days_since_release_date ASC), w_release_date_month_artist_is_4 AS (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date, id)
)
SELECT
    id,
    duration_sec,
    release_date,
    is_explicit,
    artist_debut_year,
    artist_is_verified,
    artist_id_min_duration_sec,
    last_known_release_date_month,
    min_days_since_release_date_so_far,
    bpm_max_7,
    duration_sec_max_3,
    bpm_share_of_artist_id,
    days_since_release_date_dense_rank,
    rn,
    bpm_quintile,
    artist_debut_year_sum_3_prior,
    min_duration_sec_so_far,
    COALESCE(bpm_max_7, 0) AS bpm_max_7_filled
FROM metrics
ORDER BY bpm_max_7_filled DESC, min_days_since_release_date_so_far DESC

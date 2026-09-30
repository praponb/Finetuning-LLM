WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE NOT is_explicit
      AND ARRAY_CONTAINS(moods, 'workout')
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        s.title_ref,
        a.debut_year AS artist_debut_year
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(artist_debut_year) OVER w_release_date_month_by_artist AS prev_artist_debut_year,
        MAX(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS artist_debut_year_max_4,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY MONTH(release_date) ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        MIN(artist_debut_year) OVER w_artist_id AS artist_id_min_artist_debut_year,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_release_date, 3) AS release_date_percent_rank,
        MAX(artist_debut_year) OVER w_artist_id AS artist_id_max_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER w_artist_id, 2) AS artist_id_avg_artist_debut_year,
        LEAD(artist_debut_year, 1, 0) OVER w_release_date_month_by_artist AS next_artist_debut_year,
        LAST_VALUE(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year
    FROM combined_details_artist
    WINDOW w_release_date_month_by_artist AS (PARTITION BY MONTH(release_date) ORDER BY artist_debut_year DESC, id), w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        release_date,
        moods,
        artist_debut_year,
        artist_debut_year_max_4,
        artist_id_min_artist_debut_year,
        release_date_percent_rank,
        artist_id_max_artist_debut_year,
        next_artist_debut_year,
        last_artist_debut_year,
        NTH_VALUE(prev_artist_debut_year, 2) OVER (PARTITION BY artist_id ORDER BY artist_id_avg_artist_debut_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_artist_debut_year,
        COUNT(*) OVER w_release_date_month AS rows_in_release_date_month,
        ROUND(prev_artist_debut_year / NULLIF(SUM(prev_artist_debut_year) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 0), 4) AS prev_artist_debut_year_share_of_release_date_mon,
        FIRST_VALUE(artist_debut_year) OVER (ORDER BY next_artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        MIN(prev_artist_debut_year) OVER w_artist_id_release_date_month AS artist_id_release_date_month_min_prev_artist_deb
    FROM windowed
    WINDOW w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date)), w_artist_id_release_date_month AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date))
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        MIN(release_date) AS first_release_date,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(artist_id_release_date_month_min_prev_artist_deb, 0.5) AS median_artist_id_release_date_month_min_prev_art,
        SUM(last_artist_debut_year) AS total_last_artist_debut_year
    FROM metrics
    WHERE artist_id_max_artist_debut_year < 18406
    GROUP BY DATE_TRUNC('MONTH', release_date)
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_ids, DATE_TRUNC('MONTH', release_date_month) ORDER BY first_release_date), 3) AS first_release_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY distinct_ids, DATE_TRUNC('MONTH', release_date_month) ORDER BY first_release_date, release_date_month) AS first_release_date_seq,
        ROUND(AVG(distinct_ids) OVER (PARTITION BY distinct_ids, MONTH(first_release_date) ORDER BY total_last_artist_debut_year, release_date_month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS distinct_ids_avg_2,
        MAX(total_last_artist_debut_year) OVER (PARTITION BY distinct_ids, DATE_TRUNC('MONTH', release_date_month) ORDER BY first_release_date, release_date_month ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS total_last_artist_debut_year_max_2_prior
    FROM rollup_base
)
SELECT
    release_date_month,
    first_release_date,
    distinct_ids,
    median_artist_id_release_date_month_min_prev_art,
    total_last_artist_debut_year,
    first_release_date_percent_rank,
    distinct_ids_avg_2,
    total_last_artist_debut_year_max_2_prior,
    CASE WHEN first_release_date_seq >= 8 THEN 'A' WHEN first_release_date_seq >= 3 THEN 'B' ELSE 'C' END AS first_release_date_seq_tier,
    DATEDIFF(DATE '2025-12-31', first_release_date) AS days_since_first_release_date
FROM metrics_2
ORDER BY days_since_first_release_date DESC, total_last_artist_debut_year_max_2_prior DESC

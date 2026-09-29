WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        COALESCE(bpm, 0) AS bpm_filled,
        COALESCE(bpm, 0) AS bpm_filled_2
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-08-26' AND DATE '2025-08-02'
      AND LENGTH(TRIM(title)) >= 8
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM valid s
    JOIN artists a ON a.id = s.artist_id AND UPPER(TRIM(a.artist_name)) LIKE 'L%'
    WHERE a.is_verified = TRUE
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_id, artist_is_verified)) AS distinct_artist_country_in_artist_id_artist_is_v,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_release_date_year, 0), 4) AS duration_sec_share_of_release_date_year,
        SUM(bpm_filled) OVER (PARTITION BY artist_id, artist_is_verified) AS artist_id_artist_is_verified_sum_bpm_filled,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY YEAR(release_date))) AS distinct_artist_id_in_release_date_year,
        ROUND(bpm_filled_2 / NULLIF(SUM(bpm_filled_2) OVER w_release_date_year, 0), 4) AS bpm_filled_2_share_of_release_date_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm_filled DESC, id), 3) AS bpm_filled_percent_rank,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC NULLS FIRST) AS artist_debut_year_dense_rank,
        LAG(release_date) OVER (ORDER BY bpm, id) AS prev_release_date,
        ROUND(AVG(bpm) OVER (ORDER BY bpm_filled DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        MIN(bpm_filled) OVER (PARTITION BY YEAR(release_date)) AS release_date_year_min_bpm_filled,
        MAX(bpm_filled) OVER (ORDER BY bpm_filled DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_filled_max_6_prior
    FROM joined_artist
    WINDOW w_release_date_year AS (PARTITION BY YEAR(release_date))
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        FLOOR(bpm_filled_max_6_prior / 100) * 100 AS bpm_filled_max_6_prior_bucket
    FROM sequenced
    WHERE distinct_artist_country_in_artist_id_artist_is_v >= 3
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date))) AS distinct_artist_country_in_release_date_month,
        SUM(bpm_bucket) OVER (PARTITION BY artist_id, artist_debut_year_dense_rank ORDER BY bpm_filled_max_6_prior_bucket, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_bucket_sum_1_prior,
        ROUND(bpm_avg_3_prior / NULLIF(SUM(bpm_avg_3_prior) OVER (PARTITION BY artist_country, DATE_TRUNC('MONTH', prev_release_date)), 0), 4) AS bpm_avg_3_prior_share_of_artist_country_prev_rel,
        ROW_NUMBER() OVER w_artist_id_artist_debut_year_2 AS seq_no,
        MAX(bpm_filled) OVER (PARTITION BY artist_id, artist_debut_year_dense_rank ORDER BY bpm_filled_max_6_prior_bucket, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS bpm_filled_max_4,
        ROUND(AVG(bpm_avg_3_prior) OVER (PARTITION BY artist_debut_year_dense_rank ORDER BY release_date_year_min_bpm_filled DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior_avg_1_prior,
        bpm_filled - LAG(bpm_filled) OVER (PARTITION BY artist_debut_year_dense_rank ORDER BY release_date_year_min_bpm_filled DESC, id) AS bpm_filled_change
    FROM derived
    WINDOW w_artist_id_artist_debut_year_2 AS (PARTITION BY artist_id, artist_debut_year_dense_rank ORDER BY bpm_filled_max_6_prior_bucket, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_artist_id_in_release_date_year,
        MAX(release_date) AS last_release_date,
        SUM(bpm_filled_percent_rank) AS total_bpm_filled_percent_rank,
        ROUND(100 * COUNT(CASE WHEN artist_country IN ('BR', 'JP', 'TH') THEN 1 END) / COUNT(*), 1) AS pct_artist_country_in_br_jp_th,
        SUM(bpm_filled_change) AS total_bpm_filled_change,
        MIN(bpm_avg_3_prior) AS min_bpm_avg_3_prior,
        COUNT(*) AS n_rows
    FROM metrics
    GROUP BY distinct_artist_id_in_release_date_year
    HAVING MAX(bpm_filled_percent_rank) > 0.1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY YEAR(last_release_date), n_rows), 0), 4) AS n_rows_share_of_last_release_date_year_n_rows,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_release_date) ORDER BY total_bpm_filled_change ASC NULLS LAST, distinct_artist_id_in_release_date_year) AS total_bpm_filled_change_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', last_release_date) ORDER BY total_bpm_filled_change ASC NULLS LAST, distinct_artist_id_in_release_date_year) AS rn
    FROM grouped
)
SELECT
    distinct_artist_id_in_release_date_year,
    total_bpm_filled_percent_rank,
    total_bpm_filled_change,
    min_bpm_avg_3_prior,
    n_rows_share_of_last_release_date_year_n_rows,
    total_bpm_filled_change_rank,
    ROUND(min_bpm_avg_3_prior * 1.5, 2) AS min_bpm_avg_3_prior_adj,
    ABS(total_bpm_filled_change - 450) AS total_bpm_filled_change_distance
FROM windowed

WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE SIZE(moods) >= 1
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.title_len,
        a.genre AS artist_genre
    FROM cleaned s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_artist_id_by_title_len AS seq_no,
        SUM(title_len) OVER (PARTITION BY artist_id) AS artist_id_sum_title_len,
        LEAD(release_date, 2) OVER w_title_len_artist_genre_by_ti AS next2_release_date,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY title_len, artist_genre ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        RANK() OVER (PARTITION BY title_len, artist_genre ORDER BY title_len) AS title_len_rank,
        LEAD(duration_sec) OVER w_all_by_duration_sec AS next_duration_sec,
        DENSE_RANK() OVER w_all_by_duration_sec_ties AS duration_sec_dense_rank,
        ROUND(AVG(title_len) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_title_len,
        DATEDIFF(release_date, LAG(release_date) OVER w_title_len_artist_genre_by_ti) AS days_since_prev_release_date,
        NTILE(5) OVER (ORDER BY title_len DESC, id) AS title_len_quintile,
        DENSE_RANK() OVER w_artist_id_by_title_len_ties AS title_len_dense_rank,
        MAX(title_len) OVER (ORDER BY title_len DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS title_len_max_1_prior,
        ROUND(PERCENT_RANK() OVER w_all_by_title_len, 3) AS title_len_percent_rank
    FROM with_details_artist
    WINDOW w_artist_id_by_title_len AS (PARTITION BY artist_id ORDER BY title_len DESC, id), w_title_len_artist_genre_by_ti AS (PARTITION BY title_len, artist_genre ORDER BY title_len, id), w_all_by_duration_sec AS (ORDER BY duration_sec DESC, id), w_all_by_duration_sec_ties AS (ORDER BY duration_sec DESC), w_artist_id_by_title_len_ties AS (PARTITION BY artist_id ORDER BY title_len DESC), w_all_by_title_len AS (ORDER BY title_len DESC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        artist_genre,
        seq_no,
        artist_id_sum_title_len,
        next2_release_date,
        title_len_rank,
        next_duration_sec,
        duration_sec_dense_rank,
        days_since_prev_release_date,
        title_len_dense_rank,
        CASE WHEN title_len_percent_rank >= 0.6 THEN 'large' WHEN title_len_percent_rank >= 0.3 THEN 'medium' ELSE 'small' END AS title_len_percent_rank_tier,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        title_len_max_1_prior - title_len_quintile AS title_len_max_1_prior_minus_title_len_quintile
    FROM with_windows
    WHERE title_len_quintile BETWEEN 1 AND 9
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        duration_sec,
        artist_genre,
        next2_release_date,
        next_duration_sec,
        duration_sec_dense_rank,
        days_since_prev_release_date,
        title_len_percent_rank_tier,
        title_len_max_1_prior_minus_title_len_quintile,
        COUNT(*) OVER w_title_len_percent_rank_tier AS rows_in_title_len_percent_rank_tier_title_len_de,
        ROW_NUMBER() OVER (PARTITION BY title_len_dense_rank, duration_sec_dense_rank ORDER BY seq_no DESC NULLS FIRST, id) AS seq_no_2
    FROM enriched
    WINDOW w_title_len_percent_rank_tier AS (PARTITION BY title_len_percent_rank_tier, title_len_dense_rank)
),
summary AS (
    -- Summarise per group
    SELECT
        YEAR(next2_release_date) AS next2_release_date_year,
        duration_sec_dense_rank,
        COUNT(*) FILTER (WHERE UPPER(TRIM(title)) LIKE 'S%') AS n_upper_trim_title_like_s,
        COUNT(DISTINCT title_len_percent_rank_tier) AS distinct_title_len_percent_rank_tiers
    FROM with_trends
    GROUP BY YEAR(next2_release_date), duration_sec_dense_rank
    HAVING SUM(seq_no_2) > 3
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY duration_sec_dense_rank, next2_release_date_year) AS rows_in_duration_sec_dense_rank_next2_release_da,
        ROUND(duration_sec_dense_rank / NULLIF(SUM(duration_sec_dense_rank) OVER (PARTITION BY duration_sec_dense_rank, next2_release_date_year), 0), 4) AS duration_sec_dense_rank_share_of_duration_sec_de,
        COUNT(*) OVER (PARTITION BY distinct_title_len_percent_rank_tiers ORDER BY distinct_title_len_percent_rank_tiers, next2_release_date_year, duration_sec_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(next2_release_date_year) IGNORE NULLS OVER (ORDER BY distinct_title_len_percent_rank_tiers, next2_release_date_year, duration_sec_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next2_release_date_year
    FROM summary
)
SELECT
    next2_release_date_year,
    n_upper_trim_title_like_s,
    distinct_title_len_percent_rank_tiers,
    rows_in_duration_sec_dense_rank_next2_release_da,
    duration_sec_dense_rank_share_of_duration_sec_de,
    running_count
FROM with_windows_2
WHERE running_count < 12
ORDER BY distinct_title_len_percent_rank_tiers NULLS LAST

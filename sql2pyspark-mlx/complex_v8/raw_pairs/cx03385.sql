WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit_label,
        s.title_clean,
        a.debut_year AS artist_debut_year
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(artist_debut_year) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit_label) AS release_date_month_is_explicit_label_sum_artist_,
        SUM(artist_debut_year) OVER (PARTITION BY is_explicit_label, artist_id) AS is_explicit_label_artist_id_sum_artist_debut_yea,
        LAG(release_date) OVER (PARTITION BY is_explicit_label ORDER BY artist_debut_year, id) AS prev_release_date,
        COUNT(*) OVER w_release_date_month_is_explic AS rows_in_release_date_month_is_explicit_label,
        NTILE(10) OVER (PARTITION BY is_explicit_label ORDER BY artist_debut_year ASC, id) AS artist_debut_year_decile,
        NTH_VALUE(release_date, 2) OVER (ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        COUNT(*) OVER (PARTITION BY is_explicit_label ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(artist_debut_year, 3, 0) OVER (PARTITION BY artist_id ORDER BY release_date, id) AS prev3_artist_debut_year,
        MAX(artist_debut_year) OVER (PARTITION BY is_explicit_label, artist_id) AS is_explicit_label_artist_id_max_artist_debut_yea,
        COUNT(*) OVER (PARTITION BY is_explicit_label, artist_id) AS rows_in_is_explicit_label_artist_id,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER w_is_explicit_label_artist_id, 0), 4) AS artist_debut_year_share_of_is_explicit_label_art,
        ROUND(AVG(artist_debut_year) OVER w_is_explicit_label_artist_id, 2) AS is_explicit_label_artist_id_avg_artist_debut_yea
    FROM with_parent_artist
    WINDOW w_release_date_month_is_explic AS (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit_label), w_is_explicit_label_artist_id AS (PARTITION BY is_explicit_label, artist_id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN running_count >= 14 THEN 'HIGH' WHEN running_count >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS running_count_tier,
        CASE WHEN prev3_artist_debut_year >= 2006 THEN 'HIGH' WHEN prev3_artist_debut_year >= 1994 THEN 'MEDIUM' ELSE 'LOW' END AS prev3_artist_debut_year_tier
    FROM with_trends
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(prev3_artist_debut_year) OVER (PARTITION BY artist_debut_year_decile, running_count_tier ORDER BY is_explicit_label_artist_id_max_artist_debut_yea DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev3_artist_debut_year,
        artist_debut_year_decile - LAG(artist_debut_year_decile) OVER w_artist_debut_year_decile_run_2 AS artist_debut_year_decile_change,
        MIN(artist_debut_year) OVER w_prev3_artist_debut_year_tier AS prev3_artist_debut_year_tier_min_artist_debut_ye,
        ROUND(running_count - AVG(running_count) OVER w_prev3_artist_debut_year_tier, 2) AS running_count_vs_prev3_artist_debut_year_tier_av
    FROM tagged
    WINDOW w_artist_debut_year_decile_run_2 AS (PARTITION BY artist_debut_year_decile, running_count_tier ORDER BY is_explicit_label_artist_id_max_artist_debut_yea DESC, id), w_prev3_artist_debut_year_tier AS (PARTITION BY prev3_artist_debut_year_tier)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        artist_id,
        prev3_artist_debut_year_tier,
        MIN(running_prev3_artist_debut_year) AS min_running_prev3_artist_debut_year,
        SUM(running_count_vs_prev3_artist_debut_year_tier_av) AS total_running_count_vs_prev3_artist_debut_year_t,
        PERCENTILE_APPROX(artist_debut_year_share_of_is_explicit_label_art, 0.5) AS median_artist_debut_year_share_of_is_explicit_la,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'E%' THEN artist_debut_year ELSE 0 END) AS artist_debut_year_upper_trim_title_like_e,
        SUM(artist_debut_year_decile) AS total_artist_debut_year_decile,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(title)) LIKE 'R%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_title_like_r
    FROM windowed
    GROUP BY artist_id, prev3_artist_debut_year_tier
    HAVING MAX(artist_debut_year_share_of_is_explicit_label_art) > 0.4
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(pct_upper_trim_title_like_r / NULLIF(SUM(pct_upper_trim_title_like_r) OVER (PARTITION BY prev3_artist_debut_year_tier), 0), 4) AS pct_upper_trim_title_like_r_share_of_prev3_artis,
        LEAD(pct_upper_trim_title_like_r) OVER (ORDER BY artist_debut_year_upper_trim_title_like_e ASC, artist_id, prev3_artist_debut_year_tier) AS next_pct_upper_trim_title_like_r
    FROM rollup_base
)
SELECT
    artist_id,
    prev3_artist_debut_year_tier,
    min_running_prev3_artist_debut_year,
    total_running_count_vs_prev3_artist_debut_year_t,
    median_artist_debut_year_share_of_is_explicit_la,
    artist_debut_year_upper_trim_title_like_e,
    pct_upper_trim_title_like_r,
    pct_upper_trim_title_like_r_share_of_prev3_artis,
    next_pct_upper_trim_title_like_r
FROM with_windows
WHERE next_pct_upper_trim_title_like_r >= 45.0
ORDER BY artist_id NULLS LAST, next_pct_upper_trim_title_like_r DESC

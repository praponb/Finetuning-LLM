WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        YEAR(release_date) AS release_date_year
    FROM tracks
    WHERE LOWER(title) LIKE '%o %'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        is_explicit,
        SUM(CASE WHEN is_explicit = FALSE THEN 1 ELSE 0 END) AS n_is_explicit_false,
        SUM(bpm) FILTER (WHERE NOT is_explicit) AS bpm_not_is_explicit,
        MAX(release_date) AS last_release_date,
        ROUND(STDDEV(bpm), 2) AS stddev_bpm,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(title)) LIKE 'R%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_title_like_r
    FROM valid_tracks
    GROUP BY 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        is_explicit,
        n_is_explicit_false,
        bpm_not_is_explicit,
        last_release_date,
        stddev_bpm,
        ROUND(PERCENT_RANK() OVER w_n_is_explicit_false_is_expli, 3) AS stddev_bpm_percent_rank,
        ROUND(bpm_not_is_explicit / NULLIF(SUM(bpm_not_is_explicit) OVER (PARTITION BY n_is_explicit_false, is_explicit), 0), 4) AS bpm_not_is_explicit_share_of_n_is_explicit_false,
        MIN(pct_upper_trim_title_like_r) OVER (PARTITION BY n_is_explicit_false, is_explicit) AS n_is_explicit_false_is_explicit_min_pct_upper_tr,
        LAG(pct_upper_trim_title_like_r, 2) OVER w_n_is_explicit_false_is_expli_3 AS prev2_pct_upper_trim_title_like_r
    FROM rollup_base
    WINDOW w_n_is_explicit_false_is_expli AS (PARTITION BY n_is_explicit_false, is_explicit ORDER BY stddev_bpm ASC), w_n_is_explicit_false_is_expli_3 AS (PARTITION BY n_is_explicit_false, is_explicit ORDER BY stddev_bpm ASC, is_explicit)
),
enriched AS (
    -- Derive helper columns
    SELECT
        is_explicit,
        n_is_explicit_false,
        bpm_not_is_explicit,
        stddev_bpm,
        n_is_explicit_false_is_explicit_min_pct_upper_tr,
        prev2_pct_upper_trim_title_like_r,
        LEAST(stddev_bpm_percent_rank, stddev_bpm) AS least_stddev_bpm_percent_rank_stddev_bpm
    FROM sequenced
    WHERE stddev_bpm_percent_rank > 0.2
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        is_explicit,
        n_is_explicit_false,
        stddev_bpm,
        n_is_explicit_false_is_explicit_min_pct_upper_tr,
        prev2_pct_upper_trim_title_like_r,
        least_stddev_bpm_percent_rank_stddev_bpm,
        NTILE(10) OVER (PARTITION BY n_is_explicit_false ORDER BY n_is_explicit_false_is_explicit_min_pct_upper_tr DESC, is_explicit) AS n_is_explicit_false_is_explicit_min_pct_upper_tr_2,
        ROUND(stddev_bpm / NULLIF(SUM(stddev_bpm) OVER (PARTITION BY n_is_explicit_false), 0), 4) AS stddev_bpm_share_of_n_is_explicit_false,
        RANK() OVER w_n_is_explicit_false_by_n_is_2 AS n_is_explicit_false_is_explicit_min_pct_upper_tr_3,
        ROW_NUMBER() OVER (PARTITION BY n_is_explicit_false ORDER BY n_is_explicit_false_is_explicit_min_pct_upper_tr DESC, is_explicit) AS rn,
        COUNT(*) OVER (PARTITION BY n_is_explicit_false) AS rows_in_n_is_explicit_false,
        stddev_bpm - LAG(stddev_bpm) OVER (ORDER BY least_stddev_bpm_percent_rank_stddev_bpm NULLS LAST, is_explicit) AS stddev_bpm_change
    FROM enriched
    WINDOW w_n_is_explicit_false_by_n_is_2 AS (PARTITION BY n_is_explicit_false ORDER BY n_is_explicit_false_is_explicit_min_pct_upper_tr DESC)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced_2
    WHERE rn > (SELECT AVG(rn) FROM sequenced_2)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM filtered
    WHERE rn <= 3
      AND n_is_explicit_false < 6
)
-- Final projection
SELECT
    is_explicit,
    n_is_explicit_false,
    stddev_bpm,
    prev2_pct_upper_trim_title_like_r,
    least_stddev_bpm_percent_rank_stddev_bpm,
    n_is_explicit_false_is_explicit_min_pct_upper_tr_2,
    rows_in_n_is_explicit_false,
    stddev_bpm_change,
    stddev_bpm + stddev_bpm_change AS stddev_bpm_plus_stddev_bpm_change,
    ABS(rn - 4) AS rn_distance
FROM top_per_group
WHERE n_is_explicit_false_is_explicit_min_pct_upper_tr_3 BETWEEN 4 AND 7
ORDER BY rn_distance DESC

WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE SIZE(moods) >= 2
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        days_since_release_date,
        least_bpm_duration_sec,
        LAST_DAY(release_date) AS release_date_month_end,
        COALESCE(least_bpm_duration_sec, 0) AS least_bpm_duration_sec_filled,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM recent_tracks
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(title_prefix)) AS title_prefix_len,
        DATE_TRUNC('QUARTER', release_date_month_end) AS release_date_month_end_quarter,
        LOWER(REGEXP_REPLACE(TRIM(title_prefix), ' ', '_')) AS title_prefix_slug
    FROM tagged
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(least_bpm_duration_sec / 50) * 50 AS least_bpm_duration_sec_bucket
    FROM labeled
    WHERE release_date_month_end >= DATE '2024-11-01'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        release_date_month_end,
        title_prefix,
        title_prefix_len,
        release_date_month_end_quarter,
        title_prefix_slug,
        least_bpm_duration_sec_bucket,
        ROUND(AVG(title_prefix_len) OVER w_release_date_month_end_month, 2) AS release_date_month_end_month_title_prefix_len_av,
        COUNT(*) OVER (PARTITION BY MONTH(release_date_month_end), title_prefix_len) AS rows_in_release_date_month_end_month_title_prefi,
        LAST_VALUE(days_since_release_date) IGNORE NULLS OVER (ORDER BY release_date_month_end_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_release_date
    FROM scored
    WINDOW w_release_date_month_end_month AS (PARTITION BY MONTH(release_date_month_end), title_prefix_len)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE rows_in_release_date_month_end_month_title_prefi > (SELECT AVG(rows_in_release_date_month_end_month_title_prefi) FROM windowed)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_title_prefix_len_by_last_kno AS last_known_days_since_release_date_dense_rank,
        ROUND(title_prefix_len / NULLIF(SUM(title_prefix_len) OVER (PARTITION BY title_prefix_len, YEAR(release_date_month_end_quarter)), 0), 4) AS title_prefix_len_share_of_title_prefix_len_relea
    FROM filtered
    WINDOW w_title_prefix_len_by_last_kno AS (PARTITION BY title_prefix_len ORDER BY last_known_days_since_release_date, id)
)
SELECT
    id,
    release_date_month_end,
    title_prefix_len,
    release_date_month_end_quarter,
    least_bpm_duration_sec_bucket,
    last_known_days_since_release_date,
    last_known_days_since_release_date_dense_rank,
    FLOOR(title_prefix_len / 2) * 2 AS title_prefix_len_bucket
FROM metrics
ORDER BY release_date_month_end DESC

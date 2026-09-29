WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE title IS NOT NULL
      AND is_explicit = FALSE
),
per_group AS (
    -- Summarise per group
    SELECT
        is_explicit,
        ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(bpm_per_duration_sec) FILTER (WHERE UPPER(TRIM(title)) LIKE 'N%') AS bpm_per_duration_sec_upper_trim_title_like_n,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        SUM(bpm_per_duration_sec) AS total_bpm_per_duration_sec,
        COUNT(*) AS row_count
    FROM base
    GROUP BY is_explicit
    HAVING COUNT(*) >= 3
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(distinct_ids / NULLIF(row_count, 0), 3) AS distinct_ids_per_row_count,
        distinct_ids + row_count AS distinct_ids_plus_row_count,
        FLOOR(stddev_duration_sec / 100) * 100 AS stddev_duration_sec_bucket
    FROM per_group
    WHERE total_bpm_per_duration_sec >= 21.0
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        distinct_ids - stddev_duration_sec AS distinct_ids_minus_stddev_duration_sec,
        CASE WHEN row_count >= 8 THEN 'A' WHEN row_count >= 4 THEN 'B' ELSE 'C' END AS row_count_tier,
        ROUND(distinct_ids / NULLIF(row_count, 0), 3) AS distinct_ids_per_row_count_2
    FROM enriched
    WHERE stddev_duration_sec_bucket < 357
)
SELECT
    is_explicit,
    stddev_duration_sec,
    total_bpm_per_duration_sec,
    row_count,
    distinct_ids_per_row_count,
    stddev_duration_sec_bucket,
    distinct_ids_minus_stddev_duration_sec,
    row_count_tier,
    distinct_ids_per_row_count_2,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM derived
ORDER BY total_bpm_per_duration_sec

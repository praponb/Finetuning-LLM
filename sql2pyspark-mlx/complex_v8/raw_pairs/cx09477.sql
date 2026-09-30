WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        LAST_DAY(release_date) AS release_date_month_end,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE release_date >= DATE '2024-07-01'
      AND LOWER(title) LIKE '%ai%'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(release_date_month_end) AS release_date_month_end_year,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM base
    WHERE release_date_month_end >= DATE '2024-12-01'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, bpm, is_explicit_flag
    FROM scored
) src
UNPIVOT (
    metric_value FOR metric_name IN (bpm, is_explicit_flag)
) unp

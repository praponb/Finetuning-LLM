WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE site_id >= 5
      AND kwh >= 46.6
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null
    FROM scoped
    WHERE voltage >= 222.1
    GROUP BY DATE_TRUNC('MONTH', read_at)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(row_count - 6) AS row_count_distance,
        FLOOR(n_quality_is_not_null / 2) * 2 AS n_quality_is_not_null_bucket,
        COALESCE(n_quality_is_not_null, 0) AS n_quality_is_not_null_filled
    FROM per_group
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE n_quality_is_not_null > (SELECT AVG(n_quality_is_not_null) FROM labeled)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE n_quality_is_not_null_filled > (SELECT AVG(n_quality_is_not_null_filled) FROM with_activity)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(read_at_month, 'MMM yyyy') AS read_at_month_label,
        DATE_TRUNC('WEEK', read_at_month) AS read_at_month_week
    FROM filtered
)
SELECT
    row_count_distance,
    n_quality_is_not_null_filled,
    SUM(n_quality_is_not_null) AS total_n_quality_is_not_null,
    COUNT(*) AS row_count_2,
    MIN(read_at_month) AS first_read_at_month
FROM scored
GROUP BY ROLLUP(row_count_distance, n_quality_is_not_null_filled)
ORDER BY row_count_distance NULLS LAST, n_quality_is_not_null_filled NULLS LAST

WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE site_id <= 3
),
stats AS (
    -- Summarise per group
    SELECT
        quality,
        YEAR(read_at) AS read_at_year,
        ROUND(SUM(kwh), 2) AS total_kwh,
        SUM(kwh) AS total_kwh_2
    FROM base_readings
    WHERE read_at >= TIMESTAMP '2025-01-31 10:00:00'
    GROUP BY quality, YEAR(read_at)
    HAVING MAX(kwh) > 16.5
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        ROUND(total_kwh_2 * 0.9, 2) AS total_kwh_2_adj
    FROM stats
),
tagged AS (
    -- Add calculated columns
    SELECT
        quality,
        read_at_year,
        total_kwh,
        quality_slug,
        LENGTH(TRIM(quality_slug)) AS quality_slug_len,
        LEAST(read_at_year, total_kwh_2_adj) AS least_read_at_year_total_kwh_2_adj,
        FLOOR(total_kwh_2 / 100) * 100 AS total_kwh_2_bucket
    FROM annotated
    WHERE total_kwh_2 BETWEEN 216.0 AND 468.0
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(read_at_year) OVER (PARTITION BY quality_slug_len), 2) AS quality_slug_len_avg_read_at_year,
        ROW_NUMBER() OVER (PARTITION BY quality, quality_slug_len ORDER BY least_read_at_year_total_kwh_2_adj ASC, quality, read_at_year) AS seq_no,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY quality_slug_len)) AS distinct_quality_in_quality_slug_len
    FROM tagged
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(total_kwh) OVER (PARTITION BY distinct_quality_in_quality_slug_len, read_at_year ORDER BY distinct_quality_in_quality_slug_len, quality, read_at_year ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS total_kwh_max_4,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY distinct_quality_in_quality_slug_len, read_at_year)) AS distinct_quality_in_distinct_quality_in_quality_,
        ROUND(PERCENT_RANK() OVER w_distinct_quality_in_quality_3, 3) AS distinct_quality_in_quality_slug_len_percent_ran,
        MIN(quality_slug_len) OVER (PARTITION BY distinct_quality_in_quality_slug_len, read_at_year) AS distinct_quality_in_quality_slug_len_read_at_yea,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY quality_slug_len NULLS LAST), 3) AS quality_slug_len_percent_rank
    FROM sequenced
    WINDOW w_distinct_quality_in_quality_3 AS (PARTITION BY distinct_quality_in_quality_slug_len, read_at_year ORDER BY distinct_quality_in_quality_slug_len, quality, read_at_year)
)
SELECT
    quality,
    read_at_year,
    total_kwh,
    quality_slug,
    quality_slug_len,
    least_read_at_year_total_kwh_2_adj,
    total_kwh_2_bucket,
    quality_slug_len_avg_read_at_year,
    seq_no,
    distinct_quality_in_quality_slug_len,
    distinct_quality_in_distinct_quality_in_quality_,
    distinct_quality_in_quality_slug_len_percent_ran,
    distinct_quality_in_quality_slug_len_read_at_yea,
    quality_slug_len_percent_rank,
    GREATEST(read_at_year, total_kwh_2_bucket) AS greatest_read_at_year_total_kwh_2_bucket,
    SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
FROM sequenced_2
WHERE total_kwh >= 216.0

WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_slug,
        si.site_name
    FROM base s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY kwh DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS voltage_sum_3_prior,
        ROUND(AVG(kwh) OVER (ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_7,
        LAG(read_at, 2) OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id) AS prev2_read_at,
        ROUND(kwh - AVG(kwh) OVER w_quality, 2) AS kwh_vs_quality_avg,
        LAG(voltage, 3) OVER w_site_id_quality_by_kwh AS prev3_voltage,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh), 3) AS kwh_percent_rank,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        LAST_VALUE(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        SIZE(COLLECT_SET(site_id) OVER w_quality) AS distinct_site_id_in_quality,
        LEAD(voltage) OVER (ORDER BY kwh, id) AS next_voltage,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_kwh)) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(PERCENT_RANK() OVER w_quality_by_read_at, 3) AS read_at_percent_rank,
        LEAD(voltage, 3) OVER (ORDER BY kwh, id) AS next3_voltage
    FROM combined_details
    WINDOW w_quality AS (PARTITION BY quality), w_site_id_quality_by_kwh AS (PARTITION BY site_id, quality ORDER BY kwh DESC, id), w_all_by_kwh AS (ORDER BY kwh, id), w_quality_by_read_at AS (PARTITION BY quality ORDER BY read_at, id)
),
stats AS (
    -- Summarise per group
    SELECT
        quality,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        MAX(kwh_percent_rank) AS max_kwh_percent_rank,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(distinct_site_id_in_quality), 2) AS stddev_distinct_site_id_in_quality,
        COUNT(*) AS n_rows
    FROM with_trends
    GROUP BY quality
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(n_rows) OVER (ORDER BY n_rows, quality ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS n_rows_avg_4,
        MIN(distinct_ids) OVER (PARTITION BY distinct_ids, n_rows) AS distinct_ids_n_rows_min_distinct_ids,
        RANK() OVER (ORDER BY n_rows, quality) AS n_rows_rank
    FROM stats
)
SELECT DISTINCT quality, n_rows, distinct_ids_n_rows_min_distinct_ids
FROM windowed
ORDER BY quality DESC NULLS FIRST, n_rows DESC

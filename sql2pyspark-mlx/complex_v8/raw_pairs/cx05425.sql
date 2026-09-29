WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE voltage > 218.2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id), 2) AS site_id_avg_kwh,
        LEAD(kwh, 1, 0) OVER (PARTITION BY site_id ORDER BY voltage, id) AS next_kwh,
        LAST_VALUE(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        LAG(read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY kwh, id) AS prev_read_at,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh
    FROM base
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(second_read_at) - UNIX_TIMESTAMP(read_at)) / 3600, 1) AS hours_read_at_to_second_read_at,
        LOWER(REGEXP_REPLACE(TRIM(read_at_label), ' ', '_')) AS read_at_label_slug
    FROM ranked
    WHERE read_at_label IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        read_at_label,
        quality_prefix,
        voltage_share_of_site_id,
        site_id_avg_kwh,
        next_kwh,
        last_voltage,
        prev_read_at,
        last_known_kwh,
        hours_read_at_to_second_read_at,
        ROUND(AVG(last_voltage) OVER (PARTITION BY site_id ORDER BY second_read_at ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS last_voltage_avg_1_prior,
        COUNT(*) OVER (PARTITION BY site_id ORDER BY second_read_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY last_known_kwh DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_sum_2_prior,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY TO_DATE(second_read_at))) AS distinct_site_id_in_second_read_at_to_date,
        LAG(prev_read_at, 3) OVER (PARTITION BY site_id ORDER BY last_known_kwh DESC, id) AS prev3_prev_read_at,
        MAX(last_voltage) OVER (PARTITION BY TO_DATE(second_read_at)) AS second_read_at_to_date_max_last_voltage,
        LAST_VALUE(second_read_at) OVER (PARTITION BY site_id ORDER BY last_known_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_second_read_at,
        FIRST_VALUE(prev_read_at) OVER (PARTITION BY site_id ORDER BY second_read_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev_read_at
    FROM labeled
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    read_at_label,
    site_id_avg_kwh,
    next_kwh,
    last_voltage,
    prev_read_at,
    last_known_kwh,
    hours_read_at_to_second_read_at,
    last_voltage_avg_1_prior,
    running_count,
    distinct_site_id_in_second_read_at_to_date,
    prev3_prev_read_at,
    last_voltage_avg_1_prior + last_known_kwh AS last_voltage_avg_1_prior_plus_last_known_kwh,
    ABS(running_count - 10) AS running_count_distance
FROM with_trends

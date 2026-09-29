WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE quality <> 'BAD'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_all_by_kwh AS kwh_rank,
        LAST_VALUE(read_at) OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh), 3) AS kwh_percent_rank,
        MAX(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_max_voltage,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_2
    FROM prepared_readings
    WINDOW w_all_by_kwh AS (ORDER BY kwh, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        quality_clean,
        last_read_at,
        last_known_voltage,
        quality_site_id_max_voltage,
        distinct_quality_in_site_id,
        first_known_read_at,
        ROUND(distinct_quality_in_site_id / NULLIF(SUM(distinct_quality_in_site_id) OVER (PARTITION BY site_id, distinct_quality_in_site_id), 0), 4) AS distinct_quality_in_site_id_share_of_site_id_dis,
        RANK() OVER (PARTITION BY distinct_quality_in_site_id, site_id ORDER BY voltage) AS voltage_rank,
        NTH_VALUE(first_known_read_at_2, 2) OVER (PARTITION BY distinct_quality_in_site_id, site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_known_read_at_2,
        DENSE_RANK() OVER (ORDER BY voltage DESC) AS voltage_dense_rank,
        COUNT(*) OVER w_quality_distinct_quality_in AS rows_in_quality_distinct_quality_in_site_id
    FROM with_windows
    WINDOW w_quality_distinct_quality_in AS (PARTITION BY quality, distinct_quality_in_site_id)
)
SELECT
    id,
    site_id,
    quality,
    quality_clean,
    last_read_at,
    last_known_voltage,
    quality_site_id_max_voltage,
    distinct_quality_in_site_id,
    first_known_read_at,
    distinct_quality_in_site_id_share_of_site_id_dis,
    voltage_dense_rank,
    rows_in_quality_distinct_quality_in_site_id,
    ABS(kwh - 63.6) AS kwh_distance,
    LENGTH(TRIM(quality_clean)) AS quality_clean_len
FROM metrics
ORDER BY distinct_quality_in_site_id DESC, kwh_distance DESC

WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 215.8
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        NTILE(4) OVER (PARTITION BY quality ORDER BY kwh DESC, id) AS kwh_quartile,
        COUNT(*) OVER w_quality AS rows_in_quality,
        MAX(kwh) OVER (PARTITION BY quality) AS quality_max_kwh,
        MIN(voltage) OVER (PARTITION BY TO_DATE(read_at)) AS read_at_to_date_min_voltage,
        SUM(kwh) OVER (PARTITION BY TO_DATE(read_at)) AS read_at_to_date_sum_kwh,
        LEAD(voltage) OVER w_quality_by_voltage AS next_voltage,
        ROUND(AVG(voltage) OVER (PARTITION BY TO_DATE(read_at)), 2) AS read_at_to_date_avg_voltage,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at
    FROM base
    WINDOW w_quality AS (PARTITION BY quality), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage DESC, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh_quartile,
        rows_in_quality,
        quality_max_kwh,
        next_voltage,
        read_at_to_date_avg_voltage,
        first_known_read_at,
        CONCAT(quality, '#', CAST(kwh_quartile AS STRING)) AS quality_ref
    FROM windowed
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(rows_in_quality - AVG(rows_in_quality) OVER (PARTITION BY site_id, kwh_quartile), 2) AS rows_in_quality_vs_site_id_kwh_quartile_avg,
        SIZE(COLLECT_SET(quality) OVER w_first_known_read_at_month) AS distinct_quality_in_first_known_read_at_month,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY next_voltage DESC) AS next_voltage_rank,
        RANK() OVER w_all_by_quality_max_kwh AS quality_max_kwh_rank,
        ROUND(next_voltage / NULLIF(SUM(next_voltage) OVER w_site_id_kwh_quartile, 0), 4) AS next_voltage_share_of_site_id_kwh_quartile,
        COUNT(*) OVER w_first_known_read_at_month AS rows_in_first_known_read_at_month
    FROM scored
    WINDOW w_first_known_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', first_known_read_at)), w_all_by_quality_max_kwh AS (ORDER BY quality_max_kwh DESC NULLS FIRST, id), w_site_id_kwh_quartile AS (PARTITION BY site_id, kwh_quartile)
)
SELECT
    id,
    read_at,
    kwh,
    quality,
    kwh_quartile,
    rows_in_quality,
    quality_max_kwh,
    next_voltage,
    read_at_to_date_avg_voltage,
    first_known_read_at,
    quality_ref,
    rows_in_quality_vs_site_id_kwh_quartile_avg,
    distinct_quality_in_first_known_read_at_month,
    next_voltage_rank,
    quality_max_kwh_rank
FROM windowed_2
ORDER BY quality DESC, id DESC

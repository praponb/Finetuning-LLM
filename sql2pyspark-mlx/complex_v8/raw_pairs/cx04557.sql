WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(voltage) OVER (PARTITION BY quality) AS quality_sum_voltage,
        DENSE_RANK() OVER (ORDER BY voltage DESC) AS voltage_dense_rank,
        LAG(read_at) OVER w_all_by_voltage AS prev_read_at,
        ROUND(CUME_DIST() OVER (ORDER BY voltage ASC, id), 3) AS voltage_cume_dist,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage DESC, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality), 2) AS voltage_vs_quality_avg,
        ROUND(AVG(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        LAST_VALUE(kwh) OVER (PARTITION BY quality ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        MIN(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        kwh - LAG(kwh) OVER (PARTITION BY quality ORDER BY voltage DESC, id) AS kwh_change,
        DENSE_RANK() OVER (ORDER BY voltage ASC, id) AS voltage_dense_rank_2
    FROM base
    WINDOW w_all_by_voltage AS (ORDER BY voltage ASC, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(voltage_cume_dist * 1.1, 2) AS voltage_cume_dist_adj,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM sequenced
    WHERE kwh < 87.1
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        kwh,
        quality_slug,
        quality_sum_voltage,
        voltage_dense_rank,
        prev_read_at,
        voltage_cume_dist,
        distinct_site_id_in_quality,
        rows_in_quality,
        voltage_avg_7,
        last_kwh,
        min_voltage_so_far,
        kwh_change,
        voltage_dense_rank_2,
        voltage_cume_dist_adj,
        quality_or_unknown,
        ROW_NUMBER() OVER w_all_by_min_voltage_so_far AS seq_no_2,
        MAX(voltage_avg_7) OVER (PARTITION BY voltage_dense_rank, quality) AS voltage_dense_rank_quality_max_voltage_avg_7,
        LEAD(read_at) OVER w_all_by_voltage_cume_dist_adj AS next_read_at
    FROM tagged
    WINDOW w_all_by_min_voltage_so_far AS (ORDER BY min_voltage_so_far DESC, id), w_all_by_voltage_cume_dist_adj AS (ORDER BY voltage_cume_dist_adj DESC, id)
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    quality_sum_voltage,
    voltage_dense_rank,
    prev_read_at,
    distinct_site_id_in_quality,
    rows_in_quality,
    voltage_avg_7,
    last_kwh,
    min_voltage_so_far,
    voltage_cume_dist_adj,
    quality_or_unknown,
    voltage_dense_rank_quality_max_voltage_avg_7,
    next_read_at,
    ROUND((UNIX_TIMESTAMP(prev_read_at) - UNIX_TIMESTAMP(next_read_at)) / 60, 1) AS minutes_next_read_at_to_prev_read_at,
    voltage_dense_rank + min_voltage_so_far AS voltage_dense_rank_plus_min_voltage_so_far
FROM sequenced_2

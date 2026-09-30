WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        COUNT(*) OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality), 0), 4) AS voltage_share_of_quality,
        SUM(kwh) OVER w_quality AS quality_sum_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY kwh DESC, id), 3) AS kwh_percent_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY quality), 2) AS quality_avg_voltage,
        ROW_NUMBER() OVER w_site_id_by_kwh AS kwh_seq,
        MAX(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        DENSE_RANK() OVER (ORDER BY voltage, id) AS voltage_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh), 3) AS kwh_cume_dist
    FROM valid
    WINDOW w_quality AS (PARTITION BY quality), w_site_id_by_kwh AS (PARTITION BY site_id ORDER BY kwh DESC, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        distinct_site_id_in_quality,
        voltage_share_of_quality,
        kwh_percent_rank,
        quality_avg_voltage,
        kwh_seq,
        rows_in_quality,
        voltage_dense_rank,
        kwh_cume_dist,
        QUARTER(read_at) AS read_at_quarter
    FROM metrics
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    distinct_site_id_in_quality,
    voltage_share_of_quality,
    kwh_seq,
    rows_in_quality,
    kwh_cume_dist,
    read_at_quarter,
    ROUND(kwh_seq / NULLIF(kwh, 0), 3) AS kwh_seq_per_kwh,
    GREATEST(kwh_seq, read_at_quarter) AS greatest_kwh_seq_read_at_quarter
FROM scored
ORDER BY kwh_seq, id
LIMIT 20

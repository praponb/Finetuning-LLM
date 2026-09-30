WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
      AND site_id >= 6
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        read_at_label,
        LAG(read_at, 3) OVER (PARTITION BY quality_or_unknown ORDER BY kwh DESC, id) AS prev3_read_at,
        FIRST_VALUE(voltage) OVER (PARTITION BY quality ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        voltage - LAG(voltage) OVER w_quality_or_unknown_by_read_a AS voltage_change,
        SUM(kwh) OVER (PARTITION BY TO_DATE(read_at), quality_or_unknown) AS read_at_to_date_quality_or_unknown_sum_kwh,
        COUNT(*) OVER (PARTITION BY quality_or_unknown, quality) AS rows_in_quality_or_unknown_quality,
        MIN(voltage) OVER (PARTITION BY quality_or_unknown ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        MAX(kwh) OVER (PARTITION BY quality_or_unknown, quality) AS quality_or_unknown_quality_max_kwh,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY quality_or_unknown, quality), 2) AS quality_or_unknown_quality_avg_kwh,
        ROUND(CUME_DIST() OVER w_quality_or_unknown_by_read_a_2, 3) AS read_at_cume_dist,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality_or_unknown, quality), 0), 4) AS kwh_share_of_quality_or_unknown_quality,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY read_at DESC NULLS FIRST) AS read_at_dense_rank
    FROM prepared
    WINDOW w_quality_or_unknown_by_read_a AS (PARTITION BY quality_or_unknown ORDER BY read_at, id), w_quality_or_unknown_by_read_a_2 AS (PARTITION BY quality_or_unknown ORDER BY read_at ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    voltage,
    read_at_label,
    rows_in_quality_or_unknown_quality,
    min_voltage_so_far,
    quality_or_unknown_quality_avg_kwh,
    kwh_share_of_quality_or_unknown_quality,
    read_at_dense_rank,
    LEAST(read_at_dense_rank, voltage) AS least_read_at_dense_rank_voltage
FROM with_windows
ORDER BY rows_in_quality_or_unknown_quality DESC, kwh_share_of_quality_or_unknown_quality

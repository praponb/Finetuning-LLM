WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE quality <> 'EST'
      AND read_at >= TIMESTAMP '2025-01-25 13:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(read_at_plus_30m) ORDER BY kwh DESC), 3) AS kwh_cume_dist,
        LAG(days_since_read_at, 2) OVER (PARTITION BY TO_DATE(read_at_plus_30m) ORDER BY kwh DESC, id) AS prev2_days_since_read_at,
        NTILE(3) OVER (ORDER BY voltage, id) AS voltage_tercile,
        SUM(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_sum_7,
        DENSE_RANK() OVER (ORDER BY voltage, id) AS voltage_dense_rank,
        COUNT(*) OVER (PARTITION BY quality, TO_DATE(read_at_plus_30m)) AS rows_in_quality_read_at_plus_30m_to_date,
        NTH_VALUE(read_at_plus_30m, 2) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_plus_30m,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, TO_DATE(read_at_plus_30m)), 0), 4) AS kwh_share_of_quality_read_at_plus_30m_to_date,
        ROUND(CUME_DIST() OVER (ORDER BY voltage), 3) AS voltage_cume_dist,
        ROUND(AVG(days_since_read_at) OVER (ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_read_at_avg_7,
        MAX(voltage) OVER (PARTITION BY TO_DATE(read_at_plus_30m) ORDER BY kwh DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        SUM(days_since_read_at) OVER (PARTITION BY TO_DATE(read_at_plus_30m) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_read_at
    FROM filtered_readings
    WINDOW w_quality_read_at_plus_30m_to AS (PARTITION BY quality, TO_DATE(read_at_plus_30m))
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality,
    read_at_plus_30m,
    days_since_read_at,
    prev2_days_since_read_at,
    voltage_tercile,
    voltage_dense_rank,
    rows_in_quality_read_at_plus_30m_to_date,
    second_read_at_plus_30m,
    kwh_share_of_quality_read_at_plus_30m_to_date,
    voltage_cume_dist,
    days_since_read_at_avg_7,
    running_days_since_read_at,
    CONCAT(quality, '#', CAST(days_since_read_at AS STRING)) AS quality_ref,
    FLOOR(voltage_tercile / 2) * 2 AS voltage_tercile_bucket
FROM with_windows
WHERE voltage_max_3 BETWEEN 341.2 AND 577.5
ORDER BY days_since_read_at_avg_7 DESC

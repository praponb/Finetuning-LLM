WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 07:00:00'
      AND site_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY kwh NULLS LAST), 3) AS kwh_cume_dist,
        ROUND(AVG(voltage) OVER (PARTITION BY quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        NTILE(3) OVER w_quality_by_kwh AS kwh_tercile,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh NULLS LAST, id) AS seq_no,
        SUM(kwh) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh
    FROM cleaned_readings
    WINDOW w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh NULLS LAST, id)
)
-- Subtotals with CUBE
SELECT
    kwh_tercile,
    quality,
    seq_no,
    GROUPING(kwh_tercile) AS is_total_kwh_tercile,
    COUNT(*) AS cnt
FROM with_windows
GROUP BY CUBE(kwh_tercile, quality, seq_no)
ORDER BY kwh_tercile NULLS LAST, quality NULLS LAST, seq_no NULLS LAST

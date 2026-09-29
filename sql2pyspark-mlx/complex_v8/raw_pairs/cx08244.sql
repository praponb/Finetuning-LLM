WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE quality <> 'EST'
      AND read_at >= TIMESTAMP '2025-01-25 09:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        read_at_label,
        ROUND(AVG(voltage) OVER (ORDER BY read_at ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4,
        NTILE(10) OVER (ORDER BY read_at ASC NULLS LAST, id) AS read_at_decile,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id, quality ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        MIN(kwh) OVER (PARTITION BY site_id, MONTH(read_at)) AS site_id_read_at_month_min_kwh
    FROM valid
)
SELECT
    id,
    read_at,
    read_at_decile,
    second_read_at,
    site_id_read_at_month_min_kwh,
    FLOOR(voltage_avg_4 / 5) * 5 AS voltage_avg_4_bucket
FROM windowed
WHERE site_id_read_at_month_min_kwh < 960.0
ORDER BY read_at DESC, site_id_read_at_month_min_kwh DESC

WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 101.2
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM scoped
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 81.1
        )
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_group,
        least_voltage_kwh,
        LAST_VALUE(voltage) OVER (PARTITION BY quality ORDER BY least_voltage_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id
    FROM qualified
)
SELECT
    COALESCE(quality_group, 'ALL') AS quality_group,
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    GROUPING(quality_group) AS is_total_quality_group,
    MAX(read_at) AS last_read_at
FROM ranked
GROUP BY CUBE(quality_group, quality, site_id)
ORDER BY quality_group NULLS LAST, quality NULLS LAST, site_id NULLS LAST

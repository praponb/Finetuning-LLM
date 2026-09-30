WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DAYOFWEEK(read_at) AS read_at_dayofweek,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE voltage < 241.8
      AND site_id >= 4
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality NOT IN ('EST', 'BAD')
              AND TO_DATE(m.read_at) < DATE '2025-02-18'
        )
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.read_at_dayofweek,
        s.quality_group,
        s.voltage_filled,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM without_matches s
    JOIN meter_readings m ON m.site_id = s.site_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        read_at_dayofweek,
        quality_group,
        voltage_filled,
        reading_id,
        reading_voltage,
        reading_read_at,
        reading_kwh,
        COUNT(*) OVER w_read_at_dayofweek_site_id AS rows_in_read_at_dayofweek_site_id,
        SUM(read_at_dayofweek) OVER (PARTITION BY site_id ORDER BY kwh NULLS LAST, id, reading_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS read_at_dayofweek_sum_2_prior,
        MIN(reading_kwh) OVER (PARTITION BY read_at_dayofweek, site_id) AS read_at_dayofweek_site_id_min_reading_kwh,
        SIZE(COLLECT_SET(quality_group) OVER (PARTITION BY read_at_dayofweek, site_id)) AS distinct_quality_group_in_read_at_dayofweek_site,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY kwh NULLS LAST, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh
    FROM line_level_readings
    WINDOW w_read_at_dayofweek_site_id AS (PARTITION BY read_at_dayofweek, site_id)
)
SELECT
    distinct_quality_group_in_read_at_dayofweek_site,
    quality_group,
    read_at_dayofweek,
    COUNT(DISTINCT site_id) AS distinct_site_ids
FROM ranked
GROUP BY ROLLUP(distinct_quality_group_in_read_at_dayofweek_site, quality_group, read_at_dayofweek)

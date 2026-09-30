WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR quality IN ('EST', 'OK'))
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM scoped_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(voltage) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_2,
        NTH_VALUE(kwh, 2) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh
    FROM profiled
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        quality,
        YEAR(first_reading_read_at) AS first_reading_read_at_year,
        MIN(first_reading_read_at) AS first_first_reading_read_at,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM windowed
    WHERE voltage_avg_2 > 217.0
    GROUP BY 1, 2, 3
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.site_id,
        s.quality,
        s.first_reading_read_at_year,
        s.first_first_reading_read_at,
        s.distinct_site_ids,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM stats s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    site_id,
    quality,
    first_reading_read_at_year,
    first_first_reading_read_at,
    site_tariff,
    site_zone,
    site_commissioned_on,
    DATEDIFF(DATE '2026-01-01', site_commissioned_on) AS days_since_site_commissioned_on
FROM with_details_site
ORDER BY first_reading_read_at_year

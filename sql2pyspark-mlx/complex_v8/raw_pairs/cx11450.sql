WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 12:00:00'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        DAYOFWEEK(read_at) AS read_at_dayofweek,
        DAYOFWEEK(read_at) AS read_at_dayofweek_2
    FROM scoped_readings
    WHERE read_at >= TIMESTAMP '2025-01-31 10:00:00'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(read_at_dayofweek) OVER (PARTITION BY read_at_dayofweek, read_at_dayofweek_2 ORDER BY read_at_dayofweek DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS read_at_dayofweek_sum_3,
        COUNT(*) OVER (PARTITION BY read_at_dayofweek, read_at_dayofweek_2 ORDER BY read_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(read_at_dayofweek_2) OVER w_site_id_read_at_dayofweek, 2) AS site_id_read_at_dayofweek_avg_read_at_dayofweek_,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, read_at_dayofweek)) AS distinct_quality_in_site_id_read_at_dayofweek
    FROM enriched
    WINDOW w_site_id_read_at_dayofweek AS (PARTITION BY site_id, read_at_dayofweek)
)
SELECT *
FROM (
    SELECT distinct_quality_in_site_id_read_at_dayofweek, read_at_dayofweek, quality, site_id_read_at_dayofweek_avg_read_at_dayofweek_
    FROM metrics
) src
PIVOT (
    MIN(site_id_read_at_dayofweek_avg_read_at_dayofweek_) AS min_v, SUM(site_id_read_at_dayofweek_avg_read_at_dayofweek_) AS sum_v
    FOR quality IN ('BAD', 'EST', 'OK')
)
ORDER BY distinct_quality_in_site_id_read_at_dayofweek, read_at_dayofweek

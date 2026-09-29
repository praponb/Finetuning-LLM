WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE voltage <= 236.7
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM recent_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_group,
        reading_count,
        CASE WHEN voltage >= 236.7 THEN 'A' WHEN voltage >= 220.2 THEN 'B' ELSE 'C' END AS voltage_tier,
        LOWER(REGEXP_REPLACE(TRIM(quality_group), ' ', '_')) AS quality_group_slug
    FROM with_stats
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_ties, 3) AS read_at_percent_rank,
        SUM(reading_count) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS reading_count_sum_2,
        SUM(reading_count) OVER (PARTITION BY reading_count) AS reading_count_sum_reading_count
    FROM labeled
    WINDOW w_all_by_read_at_ties AS (ORDER BY read_at DESC)
),
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff IN ('EV', 'FLAT')
        )
)
SELECT
    site_id,
    voltage_tier,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT quality) AS distinct_qualitys
FROM without_matches
GROUP BY CUBE(site_id, voltage_tier)
ORDER BY site_id NULLS LAST, voltage_tier NULLS LAST

WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE voltage > 216.5
      AND site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM scoped s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    reading_quality_count,
    site_id,
    quality,
    MAX(kwh_filled) AS max_kwh_filled,
    ROUND(SUM(reading_quality_count) / NULLIF(SUM(kwh_filled), 0), 3) AS reading_quality_count_to_kwh_filled_ratio
FROM with_counts
GROUP BY ROLLUP(reading_quality_count, site_id, quality)
ORDER BY reading_quality_count NULLS LAST, site_id NULLS LAST, quality NULLS LAST

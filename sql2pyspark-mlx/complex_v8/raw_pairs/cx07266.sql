WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        QUARTER(read_at) AS read_at_quarter,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE voltage <= 238.8
      AND kwh > 37.1
),
joined_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.site_name
    FROM valid s
    LEFT JOIN sites si ON si.id = s.site_id AND YEAR(si.commissioned_on) = 2025
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(voltage) OVER (ORDER BY voltage ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id
    FROM joined_site
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM ranked s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.read_at_quarter,
        s.site_name,
        s.reading_count,
        si.commissioned_on AS site_commissioned_on
    FROM with_counts s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    read_at_quarter,
    site_name,
    site_commissioned_on
FROM combined_details
ORDER BY site_commissioned_on DESC

WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE (kwh > 37.1 OR site_id >= 8)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(CASE WHEN quality NOT IN ('OK', 'EST') THEN 1 END) AS reading_quality_count,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.first_reading_read_at
    FROM cleaned_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_per_kwh,
        reading_count,
        reading_quality_count,
        first_reading_read_at,
        ROUND(AVG(voltage_per_kwh) OVER (PARTITION BY reading_count ORDER BY kwh_per_voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_per_kwh_avg_1_prior,
        MIN(voltage) OVER (PARTITION BY YEAR(first_reading_read_at)) AS first_reading_read_at_year_min_voltage,
        ROUND(CUME_DIST() OVER w_all_by_first_reading_read_at, 3) AS first_reading_read_at_cume_dist
    FROM with_counts
    WINDOW w_all_by_first_reading_read_at AS (ORDER BY first_reading_read_at DESC)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(voltage_per_kwh_avg_1_prior) OVER w_quality AS quality_max_voltage_per_kwh_avg_1_prior,
        SUM(voltage_per_kwh) OVER (PARTITION BY reading_count, quality ORDER BY read_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_per_kwh_sum_1_prior
    FROM sequenced
    WINDOW w_quality AS (PARTITION BY quality)
),
quality_stats AS (
    -- Median kwh per quality
    SELECT quality, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM ranked
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM ranked s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh >= g.median_kwh
)
SELECT
    id,
    read_at,
    voltage,
    reading_count,
    first_reading_read_at,
    voltage_per_kwh_avg_1_prior,
    first_reading_read_at_year_min_voltage,
    quality_max_voltage_per_kwh_avg_1_prior,
    voltage_per_kwh_sum_1_prior,
    median_kwh,
    kwh_minus_median,
    CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
    SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
FROM above_average

WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM filtered_readings
),
quality_stats AS (
    -- Max voltage per quality
    SELECT quality, MAX(voltage) AS max_voltage
    FROM annotated
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM annotated s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage = g.max_voltage
),
latest_per_key AS (
    -- Keep the latest row per quality
    SELECT
        id,
        voltage,
        quality,
        read_at_year,
        days_since_read_at,
        max_voltage,
        voltage_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage_minus_max DESC, id DESC) AS rn
        FROM above_average
    ) ranked
    WHERE rn = 1
),
totals AS (
    -- Group-level totals
    SELECT
        read_at_year,
        CASE WHEN days_since_read_at >= 420 THEN 'HIGH' WHEN days_since_read_at >= 245 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_read_at_tier,
        quality,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        ROUND(SUM(days_since_read_at) / NULLIF(SUM(max_voltage), 0), 3) AS days_since_read_at_to_max_voltage_ratio
    FROM latest_per_key
    GROUP BY 1, 2, 3
    HAVING SUM(read_at_year) > 2024
),
enriched AS (
    -- Label and score rows
    SELECT
        read_at_year,
        days_since_read_at_tier,
        quality,
        distinct_ids,
        distinct_qualitys,
        FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket
    FROM totals
    WHERE distinct_qualitys >= 4
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(distinct_qualitys / 2) * 2 AS distinct_qualitys_bucket,
        INITCAP(TRIM(days_since_read_at_tier)) AS days_since_read_at_tier_clean,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM enriched
    WHERE distinct_qualitys > 3
)
SELECT
    read_at_year,
    days_since_read_at_tier,
    distinct_qualitys,
    distinct_ids_bucket,
    days_since_read_at_tier_clean,
    quality_or_unknown,
    distinct_qualitys_bucket - distinct_ids_bucket AS distinct_qualitys_bucket_minus_distinct_ids_buck
FROM derived
WHERE quality = 'OK'
ORDER BY days_since_read_at_tier_clean DESC

WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 109.0 THEN 'A' WHEN kwh >= 37.0 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE kwh BETWEEN 24.5 AND 116.8
      AND site_id >= 6
),
unique_rows AS (
    -- Keep the earliest row per kwh_tier
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY kwh_tier ORDER BY read_at, id) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM unique_rows s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(first_reading_read_at) IGNORE NULLS OVER (PARTITION BY MONTH(read_at), quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_reading_read_at,
        ROUND(AVG(voltage) OVER (PARTITION BY kwh_tier, site_id ORDER BY first_reading_read_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        LAG(first_reading_read_at) OVER (PARTITION BY MONTH(read_at), quality ORDER BY read_at, id) AS prev_first_reading_read_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY kwh_tier, site_id ORDER BY kwh ASC, id), 3) AS kwh_percent_rank
    FROM summarized_rows
)
-- Subtotals with CUBE
SELECT
    COALESCE(kwh_tier, 'ALL') AS kwh_tier,
    COALESCE(quality, 'ALL') AS quality,
    GROUPING(kwh_tier) AS is_total_kwh_tier,
    ROUND(100 * COUNT(CASE WHEN quality = 'EST' THEN 1 END) / COUNT(*), 1) AS pct_quality_est,
    MIN(voltage) AS min_voltage,
    COUNT(*) FILTER (WHERE quality = 'OK') AS n_quality_ok
FROM windowed
GROUP BY CUBE(kwh_tier, quality)
ORDER BY kwh_tier NULLS LAST, quality NULLS LAST

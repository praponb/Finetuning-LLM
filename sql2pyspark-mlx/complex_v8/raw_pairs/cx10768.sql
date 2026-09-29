WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 10:00:00'
),
joined AS (
    -- Attach site attributes
    SELECT s.*, si.site_name, si.commissioned_on AS site_commissioned_on
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
),
totals AS (
    -- Group-level totals
    SELECT
        quality_or_unknown,
        CASE WHEN kwh >= 92.5 THEN 'HIGH' WHEN kwh >= 34.1 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        SUM(CASE WHEN UPPER(TRIM(site_name)) LIKE 'S%' THEN kwh_filled ELSE 0 END) AS kwh_filled_upper_trim_site_name_like_s,
        MAX(kwh) AS max_kwh,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        ROUND(STDDEV(kwh_filled), 2) AS stddev_kwh_filled,
        ROUND(STDDEV(voltage), 2) AS stddev_voltage
    FROM joined
    GROUP BY quality_or_unknown, CASE WHEN kwh >= 92.5 THEN 'HIGH' WHEN kwh >= 34.1 THEN 'MEDIUM' ELSE 'LOW' END, DATE_TRUNC('MONTH', read_at)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(stddev_kwh_filled) OVER w_kwh_tier_distinct_site_ids AS kwh_tier_distinct_site_ids_min_stddev_kwh_filled,
        SIZE(COLLECT_SET(quality_or_unknown) OVER (PARTITION BY kwh_tier, distinct_site_ids)) AS distinct_quality_or_unknown_in_kwh_tier_distinct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality_or_unknown ORDER BY distinct_site_ids), 3) AS distinct_site_ids_percent_rank
    FROM totals
    WINDOW w_kwh_tier_distinct_site_ids AS (PARTITION BY kwh_tier, distinct_site_ids)
)
-- Final projection
SELECT
    quality_or_unknown,
    kwh_tier,
    read_at_month,
    max_kwh,
    distinct_site_ids,
    stddev_kwh_filled,
    kwh_tier_distinct_site_ids_min_stddev_kwh_filled,
    distinct_site_ids_percent_rank,
    LOWER(TRIM(quality_or_unknown)) AS quality_or_unknown_clean,
    ABS(stddev_voltage - 110.2) AS stddev_voltage_distance
FROM windowed
WHERE kwh_tier_distinct_site_ids_min_stddev_kwh_filled >= 480.0
ORDER BY quality_or_unknown DESC, kwh_tier, read_at_month
LIMIT 20

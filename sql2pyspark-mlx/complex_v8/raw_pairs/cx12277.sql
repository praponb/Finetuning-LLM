WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 104.8 THEN 'HIGH' WHEN kwh >= 45.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        ABS(kwh - 52.4) AS kwh_distance
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 13:00:00'
      AND quality IN ('OK', 'EST')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(read_at), kwh_tier) AS rows_in_read_at_year_kwh_tier,
        NTILE(3) OVER (PARTITION BY quality ORDER BY voltage DESC, id) AS voltage_tercile,
        MAX(kwh) OVER (PARTITION BY YEAR(read_at), kwh_tier) AS read_at_year_kwh_tier_max_kwh,
        LAG(voltage) OVER w_quality_by_read_at AS prev_voltage,
        SIZE(COLLECT_SET(quality) OVER w_read_at_year_kwh_tier) AS distinct_quality_in_read_at_year_kwh_tier
    FROM filtered
    WINDOW w_quality_by_read_at AS (PARTITION BY quality ORDER BY read_at, id), w_read_at_year_kwh_tier AS (PARTITION BY YEAR(read_at), kwh_tier)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage_tercile - AVG(voltage_tercile) OVER w_distinct_quality_in_read_at, 2) AS voltage_tercile_vs_distinct_quality_in_read_at_y,
        ROUND(AVG(voltage_tercile) OVER (ORDER BY distinct_quality_in_read_at_year_kwh_tier NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_tercile_avg_1_prior
    FROM ranked
    WINDOW w_distinct_quality_in_read_at AS (PARTITION BY distinct_quality_in_read_at_year_kwh_tier, kwh_tier)
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM sequenced s
    LEFT JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        QUARTER(site_commissioned_on) AS site_commissioned_on_quarter,
        CONCAT(site_zone, '#', CAST(voltage_tercile AS STRING)) AS site_zone_ref
    FROM enriched
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        quality,
        kwh_tier,
        kwh_distance,
        rows_in_read_at_year_kwh_tier,
        voltage_tercile,
        prev_voltage,
        distinct_quality_in_read_at_year_kwh_tier,
        voltage_tercile_vs_distinct_quality_in_read_at_y,
        voltage_tercile_avg_1_prior,
        site_installed_kw,
        site_zone,
        site_commissioned_on_quarter,
        site_zone_ref,
        LENGTH(TRIM(quality)) AS quality_len,
        SUBSTRING(TRIM(kwh_tier), 1, 3) AS kwh_tier_prefix,
        COALESCE(kwh_distance, 0.0) AS kwh_distance_filled
    FROM tagged
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.kwh_tier,
        s.rows_in_read_at_year_kwh_tier,
        s.distinct_quality_in_read_at_year_kwh_tier,
        s.site_zone,
        s.site_zone_ref,
        s.quality_len,
        s.kwh_distance_filled,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM enriched_2 s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT *
FROM (
    SELECT kwh_tier, site_zone, distinct_quality_in_read_at_year_kwh_tier
    FROM line_level
) src
PIVOT (
    MAX(distinct_quality_in_read_at_year_kwh_tier)
    FOR site_zone IN ('Z3', 'Z2')
)
ORDER BY kwh_tier

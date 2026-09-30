WITH cleaned AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        LENGTH(TRIM(zone)) AS zone_len,
        CASE WHEN installed_kw >= 284.0 THEN 'large' WHEN installed_kw >= 141.6 THEN 'medium' ELSE 'small' END AS installed_kw_tier
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'M%'
      AND installed_kw > 141.6
),
zone_stats AS (
    -- Median zone_len per zone
    SELECT zone, PERCENTILE_APPROX(zone_len, 0.5) AS median_zone_len
    FROM cleaned
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_zone_len,
        ROUND(s.zone_len - g.median_zone_len, 2) AS zone_len_minus_median
    FROM cleaned s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.zone_len >= g.median_zone_len
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        zone_len,
        installed_kw_tier,
        median_zone_len,
        zone_len_minus_median,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), installed_kw_tier) AS rows_in_commissioned_on_month_installed_kw_tier,
        NTILE(10) OVER (PARTITION BY zone_len, tariff ORDER BY installed_kw, id) AS installed_kw_decile,
        LAG(zone_len) OVER (PARTITION BY zone_len, tariff ORDER BY installed_kw, id) AS prev_zone_len,
        LAG(zone_len_minus_median, 3) OVER (ORDER BY commissioned_on DESC, id) AS prev3_zone_len_minus_median
    FROM outliers
    WINDOW w_commissioned_on_month_instal AS (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), installed_kw_tier)
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.id) AS min_reading_kwh,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count,
        (SELECT COUNT(*) FILTER (WHERE m.quality IN ('BAD', 'OK')) FROM meter_readings m WHERE m.site_id = s.id) AS reading_quality_count
    FROM with_windows s
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS min_reading_voltage,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.id) AS avg_reading_kwh
    FROM with_stats s
)
SELECT id, field, amount
FROM (
    SELECT id, reading_quality_count, reading_count
    FROM profiled
) src
UNPIVOT (
    amount FOR field IN (reading_quality_count AS m_reading_quality_count, reading_count AS m_reading_count)
) unp

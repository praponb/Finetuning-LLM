WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE (MONTH(read_at) IN (1, 2) OR voltage >= 223.3)
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality = 'BAD'
        )
      AND s.voltage BETWEEN 220.2 AND 243.7
),
joined_site AS (
    -- Join sites
    SELECT s.*, si.site_name, si.installed_kw AS site_installed_kw
    FROM with_activity s
    LEFT JOIN sites si ON si.id = s.site_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        site_name,
        site_installed_kw,
        COALESCE(voltage, 0.0) AS voltage_filled,
        site_installed_kw + voltage AS site_installed_kw_plus_voltage
    FROM joined_site
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(voltage) AS min_reading_voltage,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage,
        m.last_reading_read_at
    FROM scored s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        site_name,
        site_installed_kw,
        voltage_filled,
        site_installed_kw_plus_voltage,
        min_reading_voltage,
        last_reading_read_at,
        SUBSTRING(TRIM(site_name), 1, 2) AS site_name_prefix,
        SUBSTRING(TRIM(site_name), 1, 4) AS site_name_prefix_2,
        ROUND(voltage_filled * 1.07, 2) AS voltage_filled_adj
    FROM with_stats
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        DATE_TRUNC('MONTH', last_reading_read_at) AS last_reading_read_at_month,
        PERCENTILE_APPROX(voltage_filled_adj, 0.5) AS median_voltage_filled_adj,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(voltage_filled) AS total_voltage_filled,
        ROUND(STDDEV(site_installed_kw), 2) AS stddev_site_installed_kw,
        COUNT(*) FILTER (WHERE site_name IS NOT NULL) AS n_site_name_is_not_null
    FROM enriched
    WHERE site_installed_kw_plus_voltage > 190.0
    GROUP BY DATE_FORMAT(read_at, 'yyyy-MM'), DATE_TRUNC('MONTH', last_reading_read_at)
    HAVING MAX(site_installed_kw) > 141.6
)
-- Subtotals with CUBE
SELECT
    n_site_name_is_not_null,
    cnt,
    GROUPING_ID() AS grouping_level,
    MIN(cnt) AS min_cnt
FROM totals
GROUP BY CUBE(n_site_name_is_not_null, cnt)
ORDER BY n_site_name_is_not_null NULLS LAST, cnt NULLS LAST

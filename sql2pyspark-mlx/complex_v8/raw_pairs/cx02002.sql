WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
    WHERE site_id <= 8
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        RANK() OVER (PARTITION BY MONTH(read_at) ORDER BY kwh ASC) AS kwh_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(read_at_quarter) ORDER BY read_at_quarter, id) AS read_at_quarter_dense_rank,
        MAX(kwh) OVER (PARTITION BY YEAR(read_at_quarter) ORDER BY read_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        MAX(voltage) OVER (PARTITION BY YEAR(read_at_quarter) ORDER BY read_at_quarter, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_max_4,
        NTILE(10) OVER w_read_at_quarter_year_by_read AS read_at_quarter_decile
    FROM recent
    WINDOW w_read_at_quarter_year_by_read AS (PARTITION BY YEAR(read_at_quarter) ORDER BY read_at_quarter, id)
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_clean,
        s.read_at_quarter,
        s.kwh_avg_1_prior,
        s.kwh_rank,
        s.read_at_quarter_dense_rank,
        s.max_kwh_so_far,
        s.voltage_max_4,
        s.read_at_quarter_decile,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM ranked s
    JOIN sites si ON si.id = s.site_id AND UPPER(TRIM(si.site_name)) LIKE 'S%'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(CASE WHEN quality NOT IN ('BAD', 'OK') THEN 1 END) AS reading_quality_count,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM enriched_site s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_quarter,
        s.kwh_avg_1_prior,
        s.kwh_rank,
        s.max_kwh_so_far,
        s.voltage_max_4,
        s.read_at_quarter_decile,
        s.site_installed_kw,
        s.reading_quality_count,
        s.reading_count_2,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM with_counts s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at >= TIMESTAMP '2025-02-06 10:00:00'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at_quarter,
        kwh_avg_1_prior,
        kwh_rank,
        voltage_max_4,
        read_at_quarter_decile,
        site_installed_kw,
        reading_quality_count,
        reading_id,
        reading_read_at,
        ABS(site_installed_kw - 141.6) AS site_installed_kw_distance
    FROM detail_rows_readings
    WHERE reading_quality_count BETWEEN 2 AND 6
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh_avg_1_prior,
        s.voltage_max_4,
        s.reading_quality_count,
        s.reading_id,
        s.site_installed_kw_distance,
        m.id AS reading_id_2,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM enriched s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    kwh_avg_1_prior,
    reading_quality_count,
    reading_id,
    site_installed_kw_distance,
    reading_id_2,
    reading_quality,
    reading_voltage,
    CASE WHEN reading_quality IN ('BAD', 'OK') THEN 'group_a' WHEN reading_quality = 'EST' THEN 'group_b' ELSE 'other' END AS reading_quality_group
FROM exploded_detail_readings
ORDER BY reading_voltage DESC, reading_id_2

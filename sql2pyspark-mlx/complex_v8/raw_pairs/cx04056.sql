WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        LENGTH(TRIM(quality)) AS quality_len,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE kwh > 33.9
      AND read_at < TIMESTAMP '2025-02-24 14:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        days_since_read_at,
        quality_len,
        MAX(days_since_read_at) OVER (PARTITION BY site_id, quality_len) AS site_id_quality_len_max_days_since_read_at,
        COUNT(*) OVER (PARTITION BY site_id, quality_len) AS rows_in_site_id_quality_len,
        MAX(voltage) OVER (PARTITION BY site_id, quality_len) AS site_id_quality_len_max_voltage,
        DENSE_RANK() OVER (PARTITION BY quality_len, site_id ORDER BY read_at_year) AS read_at_year_dense_rank,
        SIZE(COLLECT_SET(quality) OVER w_site_id_quality_len) AS distinct_quality_in_site_id_quality_len,
        LAG(voltage, 2) OVER w_quality_len_site_id_by_read_2 AS prev2_voltage
    FROM recent_readings
    WINDOW w_site_id_quality_len AS (PARTITION BY site_id, quality_len), w_quality_len_site_id_by_read_2 AS (PARTITION BY quality_len, site_id ORDER BY read_at_year, id)
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality = 'EST') FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM with_trends s
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.days_since_read_at,
        s.quality_len,
        s.site_id_quality_len_max_days_since_read_at,
        s.site_id_quality_len_max_voltage,
        s.distinct_quality_in_site_id_quality_len,
        s.prev2_voltage,
        s.reading_quality_count,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.site_name
    FROM summarized_rows s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    distinct_quality_in_site_id_quality_len,
    quality_len,
    GROUPING_ID() AS gid,
    SUM(reading_quality_count) FILTER (WHERE site_zone <> 'Z3') AS reading_quality_count_site_zone_z3,
    ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
    COUNT(*) AS cnt
FROM with_parent
GROUP BY CUBE(distinct_quality_in_site_id_quality_len, quality_len)

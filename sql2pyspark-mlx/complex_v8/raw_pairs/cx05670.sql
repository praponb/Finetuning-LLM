WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_quality_site_id_by_read_at_t, 3) AS read_at_cume_dist,
        SUM(kwh) OVER (PARTITION BY quality, site_id ORDER BY read_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_sum_2_prior
    FROM scoped_readings
    WINDOW w_quality_site_id_by_read_at_t AS (PARTITION BY quality, site_id ORDER BY read_at)
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM ranked s
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_or_unknown,
    quality_len,
    read_at_cume_dist,
    kwh_sum_2_prior,
    first_reading_read_at,
    CONCAT_WS('|', quality_or_unknown, quality) AS quality_or_unknown_quality_label
FROM summarized_rows
ORDER BY quality_or_unknown DESC, read_at_cume_dist NULLS LAST

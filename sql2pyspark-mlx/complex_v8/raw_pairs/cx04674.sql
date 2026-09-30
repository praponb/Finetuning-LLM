WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'BAD')
      AND voltage > 218.5
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM prepared s
    JOIN meter_readings m ON m.site_id = s.site_id
),
stats AS (
    -- Summarise per group
    SELECT
        site_id,
        DATE_TRUNC('MONTH', reading_read_at) AS reading_read_at_month,
        MIN(reading_read_at) AS first_reading_read_at,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        MIN(read_at) AS first_read_at
    FROM detail_rows
    GROUP BY site_id, DATE_TRUNC('MONTH', reading_read_at)
),
tagged AS (
    -- Label and score rows
    SELECT
        site_id,
        reading_read_at_month,
        first_read_at,
        CASE WHEN distinct_site_ids >= 6 THEN 'A' WHEN distinct_site_ids >= 3 THEN 'B' ELSE 'C' END AS distinct_site_ids_tier
    FROM stats
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        site_id,
        reading_read_at_month,
        first_read_at,
        distinct_site_ids_tier,
        LEAD(reading_read_at_month) OVER w_first_read_at_month_site_id AS next_reading_read_at_month,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_read_at), site_id ORDER BY first_read_at DESC) AS first_read_at_rank,
        ROUND(PERCENT_RANK() OVER w_first_read_at_month_site_id_2, 3) AS first_read_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(reading_read_at_month) - UNIX_TIMESTAMP(LAG(reading_read_at_month) OVER w_first_read_at_month_site_id)) / 60, 1) AS minutes_since_prev_reading_read_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', first_read_at), site_id ORDER BY first_read_at DESC), 3) AS first_read_at_cume_dist
    FROM tagged
    WINDOW w_first_read_at_month_site_id AS (PARTITION BY DATE_TRUNC('MONTH', first_read_at), site_id ORDER BY first_read_at DESC, site_id, reading_read_at_month), w_first_read_at_month_site_id_2 AS (PARTITION BY DATE_TRUNC('MONTH', first_read_at), site_id ORDER BY first_read_at DESC)
)
SELECT
    site_id,
    reading_read_at_month,
    first_read_at_rank,
    minutes_since_prev_reading_read_at_month,
    first_read_at_cume_dist,
    LOWER(REGEXP_REPLACE(TRIM(distinct_site_ids_tier), ' ', '_')) AS distinct_site_ids_tier_slug
FROM with_windows

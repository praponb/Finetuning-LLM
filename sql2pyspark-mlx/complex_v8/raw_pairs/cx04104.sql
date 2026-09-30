WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM base_readings s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh BETWEEN 34.6 AND 109.3
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        voltage,
        read_at_date,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY quality_group, site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at
    FROM screened
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN voltage >= 231.6 THEN 'HIGH' WHEN voltage >= 218.2 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM windowed
    WHERE voltage < 232.7
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, SUM(kwh) AS total_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.total_reading_kwh
    FROM enriched s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at_date,
        s.second_read_at,
        s.voltage_tier,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM with_stats s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    read_at_date,
    voltage_tier,
    site_installed_kw,
    CASE WHEN site_installed_kw >= 373.3 THEN 'large' WHEN site_installed_kw >= 141.6 THEN 'medium' ELSE 'small' END AS site_installed_kw_tier,
    QUARTER(second_read_at) AS second_read_at_quarter
FROM joined
WHERE site_zone = 'Z3'
ORDER BY id NULLS LAST, site_installed_kw_tier DESC NULLS FIRST

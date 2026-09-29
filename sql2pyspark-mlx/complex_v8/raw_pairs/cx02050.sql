WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE quality = 'BAD'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(days_since_read_at) OVER (PARTITION BY YEAR(read_at), site_id) AS read_at_year_site_id_sum_days_since_read_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY days_since_read_at DESC NULLS FIRST), 3) AS days_since_read_at_percent_rank,
        COUNT(*) OVER (PARTITION BY YEAR(read_at), site_id) AS rows_in_read_at_year_site_id,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY days_since_read_at DESC NULLS FIRST, id) AS days_since_read_at_decile
    FROM valid_readings
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.days_since_read_at,
        s.days_since_read_at_percent_rank,
        s.rows_in_read_at_year_site_id,
        si.commissioned_on AS site_commissioned_on
    FROM metrics s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw <= 386.6
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_commissioned_on, quality, days_since_read_at
    FROM with_details_site
) src
PIVOT (
    MAX(days_since_read_at) AS max_v, SUM(days_since_read_at) AS sum_v
    FOR quality IN ('EST', 'BAD', 'OK')
)

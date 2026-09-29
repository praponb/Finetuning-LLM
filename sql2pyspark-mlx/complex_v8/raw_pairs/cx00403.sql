WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM meter_readings
    WHERE kwh <= 81.3
      AND voltage BETWEEN 219.6 AND 243.1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_year) ORDER BY kwh, id) AS next_read_at,
        NTILE(10) OVER (ORDER BY read_at, id) AS read_at_decile
    FROM cleaned_readings
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.site_name, si.zone AS site_zone
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        kwh - LAG(kwh) OVER (PARTITION BY site_zone ORDER BY kwh DESC, id) AS kwh_change,
        LAG(kwh) OVER (PARTITION BY site_zone ORDER BY kwh DESC, id) AS prev_kwh,
        SUM(voltage) OVER (PARTITION BY site_zone) AS site_zone_sum_voltage,
        DENSE_RANK() OVER (PARTITION BY site_zone ORDER BY kwh DESC, id) AS kwh_dense_rank,
        MIN(read_at_decile) OVER (PARTITION BY site_zone) AS site_zone_min_read_at_decile
    FROM with_parent
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at_decile, site_id, site_zone, site_zone_sum_voltage
    FROM sequenced_2
) src
PIVOT (
    AVG(site_zone_sum_voltage)
    FOR site_zone IN ('Z4', 'Z3', 'Z2')
)
ORDER BY read_at_decile, site_id

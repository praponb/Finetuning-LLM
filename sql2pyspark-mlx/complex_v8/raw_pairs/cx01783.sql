WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE site_id >= 2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        quality_prefix,
        NTILE(10) OVER w_read_at_year_by_read_at AS read_at_decile,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(PERCENT_RANK() OVER w_read_at_year_by_read_at_ties, 3) AS read_at_percent_rank,
        MIN(voltage) OVER (PARTITION BY site_id) AS site_id_min_voltage,
        RANK() OVER (PARTITION BY site_id ORDER BY kwh) AS kwh_rank,
        ROUND(kwh - AVG(kwh) OVER w_read_at_to_date_site_id, 2) AS kwh_vs_read_at_to_date_site_id_avg,
        LEAD(kwh, 2) OVER w_site_id_by_read_at AS next2_kwh,
        LAG(voltage, 1, 0) OVER (PARTITION BY site_id ORDER BY kwh, id) AS prev_voltage
    FROM recent_readings
    WINDOW w_read_at_year_by_read_at AS (PARTITION BY YEAR(read_at) ORDER BY read_at DESC, id), w_site_id AS (PARTITION BY site_id), w_read_at_year_by_read_at_ties AS (PARTITION BY YEAR(read_at) ORDER BY read_at DESC), w_read_at_to_date_site_id AS (PARTITION BY TO_DATE(read_at), site_id), w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at DESC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE kwh_rank <= 2
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality_prefix,
    read_at_decile,
    rows_in_site_id,
    read_at_percent_rank,
    kwh_rank,
    kwh_vs_read_at_to_date_site_id_avg,
    next2_kwh,
    prev_voltage
FROM leaders
ORDER BY next2_kwh, site_id

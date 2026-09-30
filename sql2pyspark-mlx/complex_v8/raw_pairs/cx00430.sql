WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('DAY', read_at) AS read_at_day,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 8 AND 14 OR quality IN ('OK', 'BAD'))
),
combined_details AS (
    -- Join sites
    SELECT s.*, si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IN ('FLAT', 'EV')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        quality,
        read_at_day,
        site_tariff,
        LEAD(voltage) OVER w_read_at_year_by_read_at AS next_voltage,
        SUM(kwh) OVER w_site_id AS site_id_sum_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY read_at_year ORDER BY read_at), 3) AS read_at_cume_dist,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(AVG(read_at_year) OVER (PARTITION BY site_tariff, site_id ORDER BY kwh ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS read_at_year_avg_4,
        FIRST_VALUE(read_at_year) IGNORE NULLS OVER (PARTITION BY site_tariff, site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_year,
        ROUND(AVG(kwh) OVER (ORDER BY read_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        LEAD(read_at_day, 3) OVER (PARTITION BY site_tariff, site_id ORDER BY kwh ASC, id) AS next3_read_at_day,
        ROUND(read_at_year - AVG(read_at_year) OVER (PARTITION BY site_id), 2) AS read_at_year_vs_site_id_avg
    FROM combined_details
    WINDOW w_read_at_year_by_read_at AS (PARTITION BY read_at_year ORDER BY read_at, id), w_site_id AS (PARTITION BY site_id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        quality,
        read_at_day,
        next_voltage,
        site_id_sum_kwh,
        read_at_cume_dist,
        kwh_avg_3,
        last_known_voltage,
        next3_read_at_day,
        ROUND(site_id_sum_kwh / NULLIF(SUM(site_id_sum_kwh) OVER (PARTITION BY quality, site_tariff), 0), 4) AS site_id_sum_kwh_share_of_quality_site_tariff,
        COUNT(*) OVER (PARTITION BY quality, site_tariff) AS rows_in_quality_site_tariff,
        SUM(last_known_voltage) OVER (PARTITION BY quality, site_tariff) AS quality_site_tariff_sum_last_known_voltage
    FROM metrics
)
SELECT
    id,
    read_at,
    kwh,
    next_voltage,
    read_at_cume_dist,
    kwh_avg_3,
    site_id_sum_kwh_share_of_quality_site_tariff,
    quality_site_tariff_sum_last_known_voltage,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
    DATE_TRUNC('QUARTER', read_at_day) AS read_at_day_quarter
FROM ranked
ORDER BY next_voltage, kwh DESC

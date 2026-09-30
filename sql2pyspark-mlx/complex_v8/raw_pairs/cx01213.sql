WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.zone AS site_zone, si.tariff AS site_tariff
    FROM filtered_readings s
    JOIN sites si ON si.id = s.site_id AND UPPER(TRIM(si.site_name)) LIKE 'D%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        days_since_read_at,
        quality_prefix,
        site_tariff,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY MONTH(read_at), quality ORDER BY kwh ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(days_since_read_at - AVG(days_since_read_at) OVER w_site_zone, 2) AS days_since_read_at_vs_site_zone_avg,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_zone), 2) AS kwh_vs_site_zone_avg,
        SUM(days_since_read_at) OVER w_quality AS quality_sum_days_since_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY site_tariff ORDER BY read_at, id), 3) AS read_at_cume_dist,
        LEAD(kwh) OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id) AS next_kwh,
        COUNT(*) OVER (PARTITION BY site_zone) AS rows_in_site_zone,
        ROUND(CUME_DIST() OVER (PARTITION BY site_zone, TO_DATE(read_at) ORDER BY kwh DESC, id), 3) AS kwh_cume_dist,
        MAX(days_since_read_at) OVER (PARTITION BY site_tariff ORDER BY read_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_read_at_max_1_prior
    FROM combined_details_site
    WINDOW w_site_zone AS (PARTITION BY site_zone), w_quality AS (PARTITION BY quality)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    days_since_read_at,
    quality_prefix,
    site_tariff,
    days_since_read_at_vs_site_zone_avg,
    last_known_read_at,
    kwh_vs_site_zone_avg,
    quality_sum_days_since_read_at,
    read_at_cume_dist,
    next_kwh,
    rows_in_site_zone,
    kwh_cume_dist,
    days_since_read_at_max_1_prior,
    ROUND((UNIX_TIMESTAMP(last_known_read_at) - UNIX_TIMESTAMP(read_at)) / 3600, 1) AS hours_read_at_to_last_known_read_at,
    CONCAT(quality_prefix, '#', CAST(days_since_read_at AS STRING)) AS quality_prefix_ref
FROM sequenced
ORDER BY kwh_vs_site_zone_avg DESC, quality_prefix_ref DESC NULLS FIRST, id
LIMIT 20

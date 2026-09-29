WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (site_id <= 2 OR kwh <= 109.0)
),
enriched_site AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        NTH_VALUE(kwh, 2) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY YEAR(read_at))) AS distinct_site_zone_in_read_at_year,
        COUNT(*) OVER (PARTITION BY YEAR(read_at)) AS rows_in_read_at_year,
        COUNT(*) OVER (PARTITION BY site_zone) AS rows_in_site_zone,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY kwh, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4,
        DENSE_RANK() OVER (ORDER BY read_at DESC, id) AS read_at_dense_rank,
        LEAD(kwh, 2) OVER (ORDER BY kwh DESC, id) AS next2_kwh,
        MAX(kwh) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_max_6_prior,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_ties, 3) AS read_at_percent_rank,
        ROUND(kwh - AVG(kwh) OVER w_site_zone, 2) AS kwh_vs_site_zone_avg
    FROM enriched_site
    WINDOW w_all_by_read_at_ties AS (ORDER BY read_at DESC), w_site_zone AS (PARTITION BY site_zone)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        kwh,
        first_known_voltage,
        distinct_site_zone_in_read_at_year,
        rows_in_read_at_year,
        rows_in_site_zone,
        kwh_avg_4,
        next2_kwh,
        read_at_percent_rank,
        kwh_vs_site_zone_avg,
        GREATEST(first_known_voltage, second_kwh) AS greatest_first_known_voltage_second_kwh,
        rows_in_site_zone - kwh_vs_site_zone_avg AS rows_in_site_zone_minus_kwh_vs_site_zone_avg
    FROM metrics
),
totals AS (
    -- Group-level totals
    SELECT
        distinct_site_zone_in_read_at_year,
        MAX(kwh_vs_site_zone_avg) AS max_kwh_vs_site_zone_avg,
        PERCENTILE_APPROX(greatest_first_known_voltage_second_kwh, 0.5) AS median_greatest_first_known_voltage_second_kwh
    FROM scored
    WHERE first_known_voltage >= 223.0
    GROUP BY distinct_site_zone_in_read_at_year
)
SELECT
    distinct_site_zone_in_read_at_year,
    max_kwh_vs_site_zone_avg,
    median_greatest_first_known_voltage_second_kwh
FROM totals

WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.least_voltage_kwh,
        s.voltage_filled,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM cleaned s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        voltage,
        quality,
        least_voltage_kwh,
        site_tariff,
        site_commissioned_on,
        site_name,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_tariff)) AS distinct_quality_in_site_tariff,
        COUNT(*) OVER (PARTITION BY site_tariff) AS rows_in_site_tariff,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY quality)) AS distinct_site_tariff_in_quality,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_tariff)) AS distinct_site_id_in_site_tariff,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY quality, site_id)) AS distinct_site_tariff_in_quality_site_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_tariff ORDER BY voltage DESC, id), 3) AS voltage_percent_rank,
        MIN(kwh) OVER (ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        MAX(kwh) OVER (PARTITION BY YEAR(site_commissioned_on) ORDER BY kwh, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_max_2_prior,
        ROW_NUMBER() OVER (PARTITION BY site_tariff ORDER BY voltage DESC, id) AS rn,
        LAST_VALUE(least_voltage_kwh) OVER (PARTITION BY site_id, site_tariff ORDER BY voltage_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_least_voltage_kwh,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id
    FROM joined
    WINDOW w_site_tariff AS (PARTITION BY site_tariff)
),
scored AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(site_tariff)) AS site_tariff_len
    FROM windowed
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        quality,
        least_voltage_kwh,
        distinct_site_id_in_site_tariff,
        distinct_site_tariff_in_quality_site_id,
        voltage_percent_rank,
        min_kwh_so_far,
        kwh_max_2_prior,
        last_least_voltage_kwh,
        MAX(least_voltage_kwh) OVER (PARTITION BY distinct_site_tariff_in_quality_site_id ORDER BY last_least_voltage_kwh DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS least_voltage_kwh_max_2_prior,
        SUM(last_least_voltage_kwh) OVER (PARTITION BY distinct_site_tariff_in_quality_site_id ORDER BY last_least_voltage_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_least_voltage_kwh,
        ROUND(CUME_DIST() OVER (ORDER BY voltage DESC), 3) AS voltage_cume_dist,
        MAX(kwh) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_max_6_prior,
        LAST_VALUE(voltage_percent_rank) IGNORE NULLS OVER (PARTITION BY distinct_site_tariff_in_quality_site_id ORDER BY last_least_voltage_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_percent_rank,
        ROUND(min_kwh_so_far - AVG(min_kwh_so_far) OVER (PARTITION BY distinct_site_id_in_site_tariff), 2) AS min_kwh_so_far_vs_distinct_site_id_in_site_tarif
    FROM scored
)
-- Final projection
SELECT
    id,
    kwh,
    quality,
    least_voltage_kwh,
    distinct_site_id_in_site_tariff,
    distinct_site_tariff_in_quality_site_id,
    voltage_percent_rank,
    last_least_voltage_kwh,
    running_last_least_voltage_kwh,
    voltage_cume_dist,
    kwh_max_6_prior,
    last_known_voltage_percent_rank,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
    ROUND(kwh_max_6_prior / NULLIF(kwh, 0), 3) AS kwh_max_6_prior_per_kwh
FROM windowed_2

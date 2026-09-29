WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 237.5 THEN 'large' WHEN voltage >= 222.6 THEN 'medium' ELSE 'small' END AS voltage_tier,
        ABS(voltage - 222.1) AS voltage_distance
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY voltage_tier, site_id ORDER BY site_commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on,
        ROUND(CUME_DIST() OVER w_all_by_voltage_distance, 3) AS voltage_distance_cume_dist,
        LEAD(kwh, 1, 0) OVER w_quality_voltage_tier_by_site AS next_kwh,
        DENSE_RANK() OVER w_quality_voltage_tier_by_site_2 AS site_commissioned_on_dense_rank,
        COUNT(*) OVER (PARTITION BY site_tariff, quality) AS rows_in_site_tariff_quality,
        SUM(voltage) OVER w_site_tariff_quality AS site_tariff_quality_sum_voltage,
        SUM(voltage_distance) OVER (ORDER BY voltage_distance, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS voltage_distance_sum_2,
        DENSE_RANK() OVER w_all_by_voltage_distance_ties AS voltage_distance_dense_rank
    FROM enriched_site
    WINDOW w_all_by_voltage_distance AS (ORDER BY voltage_distance, id), w_quality_voltage_tier_by_site AS (PARTITION BY quality, voltage_tier ORDER BY site_commissioned_on, id), w_quality_voltage_tier_by_site_2 AS (PARTITION BY quality, voltage_tier ORDER BY site_commissioned_on), w_site_tariff_quality AS (PARTITION BY site_tariff, quality), w_all_by_voltage_distance_ties AS (ORDER BY voltage_distance)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE site_commissioned_on_dense_rank <= 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY voltage_tier, site_tariff) AS rows_in_voltage_tier_site_tariff,
        RANK() OVER w_site_commissioned_on_dense_r AS site_commissioned_on_rank,
        FIRST_VALUE(next_kwh) IGNORE NULLS OVER (PARTITION BY site_commissioned_on_dense_rank, MONTH(site_commissioned_on) ORDER BY voltage_distance_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_kwh,
        LAST_VALUE(voltage_distance) OVER (PARTITION BY site_commissioned_on_dense_rank, MONTH(site_commissioned_on) ORDER BY voltage_distance_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage_distance,
        ROUND(voltage_distance_cume_dist - AVG(voltage_distance_cume_dist) OVER (PARTITION BY voltage_tier, site_tariff), 2) AS voltage_distance_cume_dist_vs_voltage_tier_site_,
        COUNT(*) OVER (PARTITION BY quality, site_zone ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(voltage) OVER (PARTITION BY voltage_tier, site_tariff) AS voltage_tier_site_tariff_max_voltage
    FROM top_rows
    WINDOW w_site_commissioned_on_dense_r AS (PARTITION BY site_commissioned_on_dense_rank, voltage_tier ORDER BY site_commissioned_on)
)
SELECT
    id,
    kwh,
    voltage_distance,
    site_zone,
    site_tariff,
    second_site_commissioned_on,
    voltage_distance_cume_dist,
    site_commissioned_on_dense_rank,
    rows_in_site_tariff_quality,
    site_tariff_quality_sum_voltage,
    voltage_distance_sum_2,
    site_commissioned_on_rank,
    last_voltage_distance,
    voltage_distance_cume_dist_vs_voltage_tier_site_,
    running_count,
    voltage_tier_site_tariff_max_voltage
FROM with_trends
ORDER BY site_zone, site_tariff

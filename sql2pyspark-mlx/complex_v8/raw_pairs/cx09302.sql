WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 232.7 THEN 'HIGH' WHEN voltage >= 219.6 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE quality <> 'EST'
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.voltage_tier,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-12-03'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(site_commissioned_on) OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_commissioned_on,
        NTILE(4) OVER w_site_tariff_by_kwh AS kwh_quartile,
        LAST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY site_id, site_zone ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on,
        SIZE(COLLECT_SET(site_tariff) OVER w_site_zone_voltage_tier) AS distinct_site_tariff_in_site_zone_voltage_tier,
        ROUND(AVG(kwh) OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        COUNT(*) OVER (PARTITION BY site_zone, voltage_tier) AS rows_in_site_zone_voltage_tier,
        MAX(kwh) OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7,
        LAG(kwh, 2) OVER w_site_id_site_zone_by_read_at_2 AS prev2_kwh,
        DENSE_RANK() OVER (PARTITION BY site_tariff ORDER BY kwh DESC) AS kwh_dense_rank,
        RANK() OVER w_site_zone_by_read_at_ties AS read_at_rank,
        RANK() OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY read_at ASC NULLS LAST) AS read_at_rank_2
    FROM joined_site
    WINDOW w_site_tariff_by_kwh AS (PARTITION BY site_tariff ORDER BY kwh DESC, id), w_site_zone_voltage_tier AS (PARTITION BY site_zone, voltage_tier), w_site_id_site_zone_by_read_at_2 AS (PARTITION BY site_id, site_zone ORDER BY read_at DESC NULLS FIRST, id), w_site_zone_by_read_at_ties AS (PARTITION BY site_zone ORDER BY read_at)
),
annotated AS (
    -- Derive helper columns
    SELECT *, ABS(read_at_rank - 4) AS read_at_rank_distance
    FROM with_windows
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY YEAR(site_commissioned_on), voltage_tier ORDER BY prev2_kwh DESC NULLS FIRST, id) AS prev2_kwh_quartile,
        ROW_NUMBER() OVER w_site_commissioned_on_year_vo AS prev2_kwh_seq,
        SUM(read_at_rank_2) OVER (PARTITION BY YEAR(site_commissioned_on), voltage_tier ORDER BY prev2_kwh DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS read_at_rank_2_sum_4,
        ROUND(AVG(read_at_rank_2) OVER (PARTITION BY YEAR(site_commissioned_on), voltage_tier ORDER BY prev2_kwh DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS read_at_rank_2_avg_3_prior
    FROM annotated
    WINDOW w_site_commissioned_on_year_vo AS (PARTITION BY YEAR(site_commissioned_on), voltage_tier ORDER BY prev2_kwh DESC NULLS FIRST, id)
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    voltage_tier,
    site_commissioned_on,
    site_zone,
    site_tariff,
    first_site_commissioned_on,
    kwh_quartile,
    last_known_site_commissioned_on,
    distinct_site_tariff_in_site_zone_voltage_tier,
    kwh_avg_1_prior,
    rows_in_site_zone_voltage_tier,
    kwh_max_7,
    prev2_kwh,
    kwh_dense_rank,
    read_at_rank,
    read_at_rank_2,
    read_at_rank_distance,
    prev2_kwh_seq,
    read_at_rank_2_avg_3_prior,
    CASE WHEN kwh_dense_rank >= 7 THEN 'large' WHEN kwh_dense_rank >= 4 THEN 'medium' ELSE 'small' END AS kwh_dense_rank_tier,
    CONCAT(site_tariff, '#', CAST(kwh_quartile AS STRING)) AS site_tariff_ref
FROM sequenced
ORDER BY rows_in_site_zone_voltage_tier DESC NULLS FIRST

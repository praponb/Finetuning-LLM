WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh <= 92.5
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.voltage_plus_kwh,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        site_commissioned_on,
        site_zone,
        FIRST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY site_zone ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_commissioned_on,
        SUM(voltage_plus_kwh) OVER (PARTITION BY site_zone) AS site_zone_sum_voltage_plus_kwh,
        DENSE_RANK() OVER w_site_zone_by_site_commission_2 AS site_commissioned_on_dense_rank
    FROM joined
    WINDOW w_site_zone_by_site_commission_2 AS (PARTITION BY site_zone ORDER BY site_commissioned_on, id)
),
deduped AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        site_commissioned_on,
        site_zone,
        first_known_site_commissioned_on,
        site_zone_sum_voltage_plus_kwh,
        site_commissioned_on_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS recency_rank
        FROM with_windows
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        site_zone,
        first_known_site_commissioned_on,
        site_zone_sum_voltage_plus_kwh,
        site_commissioned_on_dense_rank,
        CASE WHEN kwh >= 90.6 THEN 'large' WHEN kwh >= 52.5 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM deduped
    WHERE site_commissioned_on < DATE '2025-05-10'
)
-- Final projection
SELECT
    id,
    kwh,
    first_known_site_commissioned_on,
    site_commissioned_on_dense_rank,
    kwh_tier
FROM derived
ORDER BY site_commissioned_on_dense_rank DESC NULLS FIRST, kwh NULLS LAST, id
LIMIT 10

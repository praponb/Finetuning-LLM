WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 79.4 THEN 'HIGH' WHEN kwh >= 45.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier
    FROM meter_readings
    WHERE voltage > 215.1
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.kwh_tier,
        si.installed_kw AS site_installed_kw
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw >= 96.9
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        site_installed_kw,
        COUNT(*) OVER (PARTITION BY kwh_tier, site_id) AS rows_in_kwh_tier_site_id,
        RANK() OVER (ORDER BY site_installed_kw DESC) AS site_installed_kw_rank,
        RANK() OVER (PARTITION BY site_id ORDER BY kwh NULLS LAST) AS kwh_rank,
        LAST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh
    FROM enriched
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        last_known_kwh + site_installed_kw AS last_known_kwh_plus_site_installed_kw,
        COALESCE(kwh_rank, 0) AS kwh_rank_filled
    FROM with_windows
    WHERE rows_in_kwh_tier_site_id < 17
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, site_installed_kw, last_known_kwh_plus_site_installed_kw, last_known_kwh, kwh
    FROM enriched_2
) src
UNPIVOT (
    amount FOR measure IN (site_installed_kw, last_known_kwh_plus_site_installed_kw, last_known_kwh, kwh)
) unp

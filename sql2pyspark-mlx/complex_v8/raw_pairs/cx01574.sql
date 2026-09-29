WITH covered AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CASE WHEN kwh >= 81.1 THEN 'HIGH' WHEN kwh >= 35.9 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
    FROM covered
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        energy_cost,
        quality_slug,
        kwh_tier,
        zone_or_unknown,
        ROUND(AVG(energy_cost) OVER w_kwh_tier_zone, 2) AS kwh_tier_zone_avg_energy_cost,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY kwh_tier, zone)) AS distinct_quality_in_kwh_tier_zone
    FROM derived
    WINDOW w_kwh_tier_zone AS (PARTITION BY kwh_tier, zone)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, kwh_tier_zone_avg_energy_cost, energy_cost, kwh
    FROM with_windows
) src
UNPIVOT (
    reading FOR field IN (kwh_tier_zone_avg_energy_cost, energy_cost, kwh)
) unp

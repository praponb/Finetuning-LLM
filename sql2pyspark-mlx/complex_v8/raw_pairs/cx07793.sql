WITH covered AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
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
grouped AS (
    -- Group-level totals
    SELECT zone, SUM(kwh) AS total_kwh, COUNT(DISTINCT zone) AS distinct_zones
    FROM covered
    GROUP BY 1
    HAVING SUM(kwh) > 42.1
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(zone, '#', CAST(distinct_zones AS STRING)) AS zone_ref,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM grouped
)
SELECT
    zone,
    total_kwh,
    distinct_zones,
    zone_ref,
    zone_slug,
    COALESCE(distinct_zones, 0) AS distinct_zones_filled
FROM labeled
ORDER BY zone DESC, total_kwh DESC

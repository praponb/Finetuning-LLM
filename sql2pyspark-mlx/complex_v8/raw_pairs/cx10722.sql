WITH matched AS (
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
latest_per_key AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at, id) AS rn
        FROM matched
    ) ranked
    WHERE rn = 1
),
summary AS (
    -- Summarise per group
    SELECT
        site_id,
        zone,
        SUM(rate) AS total_rate,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list,
        MAX(read_at) AS last_read_at
    FROM latest_per_key
    GROUP BY site_id, zone
    HAVING AVG(rate) > 2.2
),
enriched_site AS (
    -- Join sites
    SELECT
        s.site_id,
        s.zone,
        s.total_rate,
        s.zone_list,
        s.last_read_at,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM summary s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    COALESCE(zone, 'ALL') AS zone,
    COALESCE(site_zone, 'ALL') AS site_zone,
    MAX(site_commissioned_on) AS last_site_commissioned_on
FROM enriched_site
GROUP BY GROUPING SETS ((site_id, zone, site_zone), (site_id), ())
ORDER BY site_id NULLS LAST, zone NULLS LAST, site_zone NULLS LAST

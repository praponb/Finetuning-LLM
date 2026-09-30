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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug,
        INITCAP(TRIM(zone)) AS zone_clean
    FROM covered
    WHERE rate < 5.7
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.rate,
        s.energy_cost,
        s.zone_slug,
        s.zone_clean,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM enriched s
    JOIN sites si ON si.id = s.site_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        energy_cost,
        zone_slug,
        site_commissioned_on,
        site_zone,
        site_tariff,
        DATE_FORMAT(site_commissioned_on, 'MMM yyyy') AS site_commissioned_on_label,
        GREATEST(energy_cost, kwh) AS greatest_energy_cost_kwh
    FROM joined
    WHERE site_tariff = 'EV'
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    energy_cost,
    zone_slug,
    site_commissioned_on,
    site_zone,
    site_tariff,
    greatest_energy_cost_kwh,
    TO_DATE(read_at) AS read_at_date
FROM scored
ORDER BY site_commissioned_on, site_tariff

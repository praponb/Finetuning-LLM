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
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.zone,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM covered s
    LEFT JOIN sites si ON si.id = s.site_id
),
latest AS (
    -- Keep the latest row per zone, site_id
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        zone,
        site_commissioned_on,
        site_installed_kw,
        site_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone, site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM with_details
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage
    FROM latest
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, kwh_share_of_site_id, voltage
    FROM with_windows
) src
UNPIVOT (
    value FOR field IN (kwh_share_of_site_id, voltage)
) unp

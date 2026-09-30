WITH joined_ranges AS (
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
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', quality, zone) AS quality_zone_label,
        INITCAP(TRIM(zone)) AS zone_clean,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM joined_ranges
    WHERE rate < 5.7
)
SELECT *
FROM (
    SELECT site_id, read_at, quality, kwh
    FROM tagged
) src
PIVOT (
    AVG(kwh)
    FOR quality IN ('OK', 'BAD', 'EST')
)
ORDER BY site_id, read_at

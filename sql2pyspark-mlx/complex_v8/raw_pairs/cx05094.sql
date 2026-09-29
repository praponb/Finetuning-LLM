WITH joined_ranges AS (
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
zone_stats AS (
    -- Max rate per zone
    SELECT zone, MAX(rate) AS max_rate
    FROM joined_ranges
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_rate, ROUND(s.rate - g.max_rate, 2) AS rate_minus_max
    FROM joined_ranges s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.rate = g.max_rate
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.rate,
        s.energy_cost,
        s.max_rate,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM outliers s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.rate,
        s.energy_cost,
        s.max_rate,
        s.reading_id,
        s.reading_voltage,
        s.reading_read_at,
        si.site_name,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        m.quality AS reading_quality
    FROM exploded_detail s
    JOIN sites si ON si.id = s.site_id
    JOIN meter_readings m ON m.id = s.reading_id
    WHERE si.zone = 'Z3'
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    rate,
    energy_cost,
    max_rate,
    reading_id,
    reading_voltage,
    reading_read_at,
    site_name,
    site_zone,
    site_commissioned_on,
    LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug,
    ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(reading_read_at)) / 3600, 1) AS hours_reading_read_at_to_read_at
FROM with_details_site
ORDER BY kwh, site_id DESC

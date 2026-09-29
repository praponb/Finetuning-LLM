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
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        m.last_reading_read_at
    FROM joined_ranges s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        quality,
        zone,
        rate,
        energy_cost,
        first_reading_read_at,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        CONCAT_WS(': ', zone, quality) AS zone_quality_label,
        CONCAT_WS(' / ', quality, zone) AS quality_zone_label
    FROM with_stats
    WHERE zone = 'Z2'
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        quality,
        zone,
        rate,
        energy_cost,
        first_reading_read_at,
        quality_prefix,
        zone_quality_label,
        quality_zone_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY energy_cost DESC, id DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    read_at,
    quality,
    zone,
    rate,
    energy_cost,
    quality_prefix,
    zone_quality_label,
    COALESCE(energy_cost, 0.0) AS energy_cost_filled,
    LENGTH(TRIM(quality_prefix)) AS quality_prefix_len
FROM latest
ORDER BY id DESC, quality

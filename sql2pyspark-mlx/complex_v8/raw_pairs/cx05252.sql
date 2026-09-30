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
scored AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM joined_ranges
    WHERE zone IN ('Z2', 'Z4')
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-18 12:00:00'
        )
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    zone,
    rate,
    energy_cost,
    days_since_read_at,
    LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
FROM qualified
ORDER BY zone_slug, days_since_read_at

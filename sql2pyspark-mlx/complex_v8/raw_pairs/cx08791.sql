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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND TO_DATE(m.read_at) < DATE '2025-02-06'
        )
      AND s.quality = 'EST'
),
unique_rows AS (
    -- Keep the earliest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY read_at, id) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Add calculated columns
    SELECT *, ROUND(energy_cost * 1.07, 2) AS energy_cost_adj
    FROM unique_rows
)
SELECT
    id,
    site_id,
    read_at,
    quality,
    energy_cost,
    energy_cost_adj,
    CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref,
    LEAST(rate, energy_cost_adj) AS least_rate_energy_cost_adj
FROM annotated

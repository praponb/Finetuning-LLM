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
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        si.site_name,
        si.zone AS site_zone
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id AND si.zone = 'Z2'
    WHERE si.zone = 'Z4'
),
grouped AS (
    -- Group-level totals
    SELECT
        site_id,
        site_zone,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        ROUND(SUM(energy_cost) / NULLIF(SUM(rate), 0), 3) AS energy_cost_to_rate_ratio,
        MAX(read_at) AS last_read_at,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN site_zone <> 'Z4' THEN 1 ELSE 0 END) AS n_site_zone_z4
    FROM with_details
    GROUP BY site_id, site_zone, DATE_TRUNC('MONTH', read_at)
)
-- Final projection
SELECT
    site_id,
    site_zone,
    read_at_month,
    energy_cost_to_rate_ratio,
    last_read_at,
    n_rows,
    LOWER(REGEXP_REPLACE(TRIM(site_zone), ' ', '_')) AS site_zone_slug,
    LOWER(TRIM(site_zone)) AS site_zone_clean
FROM grouped
ORDER BY energy_cost_to_rate_ratio DESC

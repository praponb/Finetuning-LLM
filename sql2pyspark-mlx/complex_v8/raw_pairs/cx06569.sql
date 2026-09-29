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
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(energy_cost) IGNORE NULLS OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_energy_cost,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY site_id), 0), 4) AS rate_share_of_site_id,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY site_id)) AS distinct_zone_in_site_id,
        MIN(rate) OVER w_site_id AS site_id_min_rate
    FROM covered
    WINDOW w_site_id AS (PARTITION BY site_id)
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-24 10:00:00'
        )
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(kwh / NULLIF(first_known_energy_cost, 0), 3) AS kwh_per_first_known_energy_cost,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM without_matches
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, zone, rate_share_of_site_id
    FROM annotated
) src
PIVOT (
    MAX(rate_share_of_site_id)
    FOR zone IN ('Z3', 'Z2')
)

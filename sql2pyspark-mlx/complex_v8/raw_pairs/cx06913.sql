WITH in_window AS (
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
enriched AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        zone,
        rate,
        energy_cost,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        LOWER(TRIM(zone)) AS zone_clean,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM in_window
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        zone,
        energy_cost,
        zone_clean,
        read_at_dayofweek,
        NTH_VALUE(rate, 2) OVER (ORDER BY energy_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate,
        ROUND(energy_cost - AVG(energy_cost) OVER (PARTITION BY zone, site_id), 2) AS energy_cost_vs_zone_site_id_avg,
        SUM(rate) OVER (PARTITION BY zone, site_id) AS zone_site_id_sum_rate
    FROM enriched
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE read_at_dayofweek > (SELECT AVG(read_at_dayofweek) FROM metrics)
)
-- Final projection
SELECT
    id,
    zone,
    energy_cost,
    read_at_dayofweek,
    zone_site_id_sum_rate
FROM filtered
WHERE LENGTH(TRIM(zone_clean)) >= 8
ORDER BY zone DESC

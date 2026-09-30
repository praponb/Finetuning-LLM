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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, YEAR(read_at) ORDER BY kwh DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        SUM(energy_cost) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_energy_cost,
        RANK() OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at DESC) AS read_at_rank,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at DESC, id) AS rn,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh
    FROM covered
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        kwh_avg_3,
        running_energy_cost,
        rn,
        first_known_kwh
    FROM metrics
    WHERE read_at_rank <= 2
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    zone,
    rate,
    rn,
    first_known_kwh,
    ABS(kwh_avg_3 - 42.0) AS kwh_avg_3_distance,
    CASE WHEN zone IN ('Z4', 'Z3') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group
FROM top_per_group
WHERE rate <= 5.7
ORDER BY first_known_kwh DESC, zone DESC

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
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(rate) OVER (ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate,
        LEAD(kwh, 3) OVER (PARTITION BY zone, site_id ORDER BY voltage, id) AS next3_kwh
    FROM joined_ranges
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        zone,
        rate,
        FIRST_VALUE(rate) OVER (PARTITION BY zone, MONTH(read_at) ORDER BY first_rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate_2,
        MAX(energy_cost) OVER (PARTITION BY zone, MONTH(read_at) ORDER BY first_rate DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS energy_cost_max_3,
        ROUND(AVG(rate) OVER (PARTITION BY zone ORDER BY energy_cost ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rate_avg_2
    FROM metrics
)
SELECT
    id,
    site_id,
    read_at,
    zone,
    first_rate_2,
    LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
FROM with_windows
ORDER BY zone_slug DESC NULLS FIRST, site_id

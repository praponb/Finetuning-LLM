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
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        quality,
        ROUND(SUM(rate) / NULLIF(SUM(kwh), 0), 3) AS rate_to_kwh_ratio,
        MAX(energy_cost) AS max_energy_cost,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
        SUM(CASE WHEN zone = 'Z4' THEN 1 ELSE 0 END) AS n_zone_z4
    FROM covered
    GROUP BY DATE_FORMAT(read_at, 'yyyy-MM'), quality
    HAVING MAX(kwh) > 43.1
)
SELECT
    read_at_ym,
    quality,
    rate_to_kwh_ratio,
    max_energy_cost,
    median_kwh
FROM rollup_base
ORDER BY rate_to_kwh_ratio DESC NULLS FIRST

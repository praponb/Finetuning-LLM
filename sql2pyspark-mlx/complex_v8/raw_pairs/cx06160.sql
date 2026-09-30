WITH in_window AS (
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
site_id_stats AS (
    -- Median energy_cost per site_id
    SELECT site_id, PERCENTILE_APPROX(energy_cost, 0.5) AS median_energy_cost
    FROM in_window
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_energy_cost,
        ROUND(s.energy_cost - g.median_energy_cost, 2) AS energy_cost_minus_median
    FROM in_window s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.energy_cost >= g.median_energy_cost
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM outliers
    WHERE energy_cost > (SELECT MIN(energy_cost) FROM outliers)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN rate >= 6.1 THEN 'A' WHEN rate >= 4.2 THEN 'B' ELSE 'C' END AS rate_tier,
        zone,
        site_id,
        COUNT(DISTINCT zone) AS distinct_zones,
        MAX(kwh) AS max_kwh
    FROM screened
    GROUP BY 1, 2, 3
    HAVING MAX(rate) > 3.7
)
-- Final projection
SELECT
    rate_tier,
    site_id,
    distinct_zones,
    max_kwh,
    UPPER(TRIM(rate_tier)) AS rate_tier_clean
FROM grouped
ORDER BY rate_tier_clean DESC, site_id DESC NULLS FIRST

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
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM joined_ranges s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 219.6 AND 237.5
              AND HOUR(m.read_at) BETWEEN 9 AND 13
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY quality, site_id)) AS distinct_zone_in_quality_site_id,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY quality, site_id), 0), 4) AS rate_share_of_quality_site_id,
        ROUND(energy_cost - AVG(energy_cost) OVER (PARTITION BY quality, site_id), 2) AS energy_cost_vs_quality_site_id_avg,
        LAG(energy_cost) OVER (PARTITION BY quality, site_id ORDER BY rate, id) AS prev_energy_cost
    FROM qualified
)
-- Subtotals with GROUPING SETS
SELECT
    quality,
    zone,
    distinct_zone_in_quality_site_id,
    GROUPING_ID() AS gid,
    MIN(energy_cost_vs_quality_site_id_avg) AS min_energy_cost_vs_quality_site_id_avg,
    MIN(distinct_zone_in_quality_site_id) AS min_distinct_zone_in_quality_site_id
FROM sequenced
GROUP BY GROUPING SETS ((quality, zone, distinct_zone_in_quality_site_id), (quality), ())
ORDER BY quality NULLS LAST, zone NULLS LAST, distinct_zone_in_quality_site_id NULLS LAST

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
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(energy_cost - AVG(energy_cost) OVER (PARTITION BY zone), 2) AS energy_cost_vs_zone_avg,
        LAG(energy_cost, 3) OVER (PARTITION BY MONTH(read_at), quality ORDER BY energy_cost DESC, id) AS prev3_energy_cost,
        ROUND(PERCENT_RANK() OVER w_quality_by_read_at_ties, 3) AS read_at_percent_rank
    FROM joined_ranges
    WINDOW w_quality_by_read_at_ties AS (PARTITION BY quality ORDER BY read_at DESC)
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.rate,
        s.energy_cost,
        s.minutes_since_prev_read_at,
        s.energy_cost_vs_zone_avg,
        s.prev3_energy_cost,
        s.read_at_percent_rank,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM with_trends s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        rate,
        energy_cost,
        energy_cost_vs_zone_avg,
        read_at_percent_rank,
        site_installed_kw,
        site_tariff,
        COUNT(*) OVER (PARTITION BY site_id, site_tariff) AS rows_in_site_id_site_tariff,
        LAG(rate, 2, 0) OVER w_site_zone_site_id_by_read_at AS prev2_rate
    FROM with_details_site
    WINDOW w_site_zone_site_id_by_read_at AS (PARTITION BY site_zone, site_id ORDER BY read_at, id)
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    rate,
    energy_cost,
    energy_cost_vs_zone_avg,
    read_at_percent_rank,
    site_installed_kw,
    rows_in_site_id_site_tariff
FROM metrics
ORDER BY rate, read_at DESC NULLS FIRST

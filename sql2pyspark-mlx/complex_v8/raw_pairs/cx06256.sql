WITH covered AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.zone,
        s.rate,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM covered s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        zone,
        rate,
        site_commissioned_on,
        MIN(kwh) OVER (PARTITION BY zone, site_zone) AS zone_site_zone_min_kwh,
        COUNT(*) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_parent
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY zone, YEAR(read_at))) AS distinct_site_id_in_zone_read_at_year,
        ROUND(running_count - AVG(running_count) OVER w_zone_read_at_year, 2) AS running_count_vs_zone_read_at_year_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY rate DESC NULLS FIRST), 3) AS rate_cume_dist
    FROM metrics
    WINDOW w_zone_read_at_year AS (PARTITION BY zone, YEAR(read_at))
)
SELECT
    id,
    site_id,
    read_at,
    zone,
    site_commissioned_on,
    zone_site_zone_min_kwh,
    running_count_vs_zone_read_at_year_avg,
    rate_cume_dist,
    zone_site_zone_min_kwh - rate AS zone_site_zone_min_kwh_minus_rate,
    LEAST(rate, zone_site_zone_min_kwh) AS least_rate_zone_site_zone_min_kwh
FROM ranked
WHERE rate_cume_dist <= 0.9

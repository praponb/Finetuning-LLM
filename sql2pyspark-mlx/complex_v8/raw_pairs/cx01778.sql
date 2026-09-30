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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(energy_cost) OVER (PARTITION BY MONTH(read_at) ORDER BY rate DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_energy_cost,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY site_id, MONTH(read_at))) AS distinct_zone_in_site_id_read_at_month,
        NTILE(5) OVER w_read_at_month_site_id_by_kwh AS kwh_quintile,
        ROUND(rate - AVG(rate) OVER (PARTITION BY site_id, MONTH(read_at)), 2) AS rate_vs_site_id_read_at_month_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY kwh, id), 3) AS kwh_cume_dist
    FROM in_window
    WINDOW w_read_at_month_site_id_by_kwh AS (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY kwh, id)
),
deduped AS (
    -- Keep the latest row per zone, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost,
        first_energy_cost,
        distinct_zone_in_site_id_read_at_month,
        kwh_quintile,
        rate_vs_site_id_read_at_month_avg,
        kwh_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone, site_id ORDER BY read_at DESC, id DESC) AS rn
        FROM with_trends
    ) ranked
    WHERE rn = 1
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM deduped s
)
SELECT
    distinct_zone_in_site_id_read_at_month,
    kwh_quintile,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE zone = 'Z3') AS n_zone_z3,
    ROUND(SUM(energy_cost) / NULLIF(SUM(kwh), 0), 3) AS energy_cost_to_kwh_ratio
FROM with_stats
GROUP BY CUBE(distinct_zone_in_site_id_read_at_month, kwh_quintile)
ORDER BY distinct_zone_in_site_id_read_at_month NULLS LAST, kwh_quintile NULLS LAST

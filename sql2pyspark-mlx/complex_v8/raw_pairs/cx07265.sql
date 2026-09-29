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
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_read_at_year_by_energy_cost, 3) AS energy_cost_cume_dist,
        RANK() OVER (PARTITION BY YEAR(read_at) ORDER BY energy_cost) AS energy_cost_rank,
        LAST_VALUE(kwh) OVER (PARTITION BY YEAR(read_at) ORDER BY energy_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh
    FROM joined_ranges
    WINDOW w_read_at_year_by_energy_cost AS (PARTITION BY YEAR(read_at) ORDER BY energy_cost)
),
leaders AS (
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
        energy_cost_cume_dist,
        last_kwh
    FROM sequenced
    WHERE energy_cost_rank <= 2
      AND energy_cost_cume_dist BETWEEN 0.2 AND 0.7
)
SELECT
    quality,
    zone,
    site_id,
    ROUND(AVG(last_kwh), 2) AS avg_last_kwh,
    SUM(CASE WHEN zone = 'Z2' THEN 1 ELSE 0 END) AS n_zone_z2
FROM leaders
GROUP BY CUBE(quality, zone, site_id)

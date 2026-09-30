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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        rate,
        MIN(kwh) OVER (PARTITION BY zone, site_id) AS zone_site_id_min_kwh,
        SUM(kwh) OVER (PARTITION BY zone, site_id) AS zone_site_id_sum_kwh,
        ROUND(CUME_DIST() OVER w_read_at_month_site_id_by_rat, 3) AS rate_cume_dist,
        MIN(rate) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rate_so_far,
        MAX(voltage) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY rate, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_max_1_prior
    FROM joined_ranges
    WINDOW w_read_at_month_site_id_by_rat AS (PARTITION BY MONTH(read_at), site_id ORDER BY rate, id)
),
stats AS (
    -- Summarise per group
    SELECT
        TO_DATE(read_at) AS read_at_day,
        site_id,
        CASE WHEN rate_cume_dist >= 0.8 THEN 'large' WHEN rate_cume_dist >= 0.2 THEN 'medium' ELSE 'small' END AS rate_cume_dist_tier,
        ROUND(SUM(voltage_max_1_prior), 2) AS total_voltage_max_1_prior,
        ROUND(SUM(rate) / NULLIF(SUM(zone_site_id_sum_kwh), 0), 3) AS rate_to_zone_site_id_sum_kwh_ratio,
        ROUND(STDDEV(rate_cume_dist), 2) AS stddev_rate_cume_dist,
        MIN(read_at) AS first_read_at,
        PERCENTILE_APPROX(zone_site_id_min_kwh, 0.5) AS median_zone_site_id_min_kwh
    FROM with_windows
    GROUP BY TO_DATE(read_at), site_id, CASE WHEN rate_cume_dist >= 0.8 THEN 'large' WHEN rate_cume_dist >= 0.2 THEN 'medium' ELSE 'small' END
)
-- Final projection
SELECT
    read_at_day,
    site_id,
    rate_cume_dist_tier,
    total_voltage_max_1_prior,
    rate_to_zone_site_id_sum_kwh_ratio,
    stddev_rate_cume_dist,
    first_read_at,
    median_zone_site_id_min_kwh,
    COALESCE(stddev_rate_cume_dist, 0.0) AS stddev_rate_cume_dist_filled
FROM stats

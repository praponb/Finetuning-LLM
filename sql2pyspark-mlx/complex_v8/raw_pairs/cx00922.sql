WITH matched AS (
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
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        CASE WHEN rate >= 5.7 THEN 'HIGH' WHEN rate >= 4.2 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        site_id,
        MAX(voltage) AS max_voltage,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list,
        SUM(CASE WHEN zone = 'Z3' THEN rate ELSE 0 END) AS rate_zone_z3,
        ROUND(100 * COUNT(CASE WHEN zone <> 'Z2' THEN 1 END) / COUNT(*), 1) AS pct_zone_z2
    FROM matched
    GROUP BY DATE_TRUNC('MONTH', read_at), CASE WHEN rate >= 5.7 THEN 'HIGH' WHEN rate >= 4.2 THEN 'MEDIUM' ELSE 'LOW' END, site_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        read_at_month,
        rate_tier,
        site_id,
        max_voltage,
        zone_list,
        rate_zone_z3,
        pct_zone_z2,
        LAG(max_voltage) OVER (PARTITION BY site_id ORDER BY pct_zone_z2 DESC, read_at_month, rate_tier, site_id) AS prev_max_voltage,
        SUM(pct_zone_z2) OVER (ORDER BY pct_zone_z2 ASC, read_at_month, rate_tier, site_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS pct_zone_z2_sum_1_prior,
        SUM(rate_zone_z3) OVER (ORDER BY pct_zone_z2 ASC, read_at_month, rate_tier, site_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS rate_zone_z3_sum_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY pct_zone_z2 DESC, read_at_month, rate_tier, site_id), 3) AS pct_zone_z2_cume_dist,
        FIRST_VALUE(max_voltage) OVER (PARTITION BY rate_tier ORDER BY max_voltage DESC NULLS FIRST, read_at_month, rate_tier, site_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_max_voltage
    FROM stats
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        read_at_month,
        rate_tier,
        site_id,
        max_voltage,
        zone_list,
        rate_zone_z3,
        pct_zone_z2,
        pct_zone_z2_sum_1_prior,
        rate_zone_z3_sum_1_prior,
        pct_zone_z2_cume_dist,
        MAX(first_max_voltage) OVER (PARTITION BY rate_tier) AS rate_tier_max_first_max_voltage,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY rate_tier)) AS distinct_site_id_in_rate_tier,
        ROUND((UNIX_TIMESTAMP(read_at_month) - UNIX_TIMESTAMP(LAG(read_at_month) OVER (PARTITION BY rate_tier ORDER BY rate_zone_z3 DESC, read_at_month, rate_tier, site_id))) / 60, 1) AS minutes_since_prev_read_at_month
    FROM ranked
)
SELECT
    read_at_month,
    rate_tier,
    site_id,
    max_voltage,
    rate_zone_z3,
    pct_zone_z2_sum_1_prior,
    pct_zone_z2_cume_dist,
    rate_tier_max_first_max_voltage,
    minutes_since_prev_read_at_month,
    CASE WHEN rate_zone_z3 >= 32.0 THEN 'HIGH' WHEN rate_zone_z3 >= 14.0 THEN 'MEDIUM' ELSE 'LOW' END AS rate_zone_z3_tier,
    TO_DATE(read_at_month) AS read_at_month_date
FROM sequenced
WHERE rate_tier NOT IN ('LOW', 'MEDIUM')
ORDER BY pct_zone_z2_cume_dist, read_at_month

WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 220.2 AND 241.9
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.site_name
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DATEDIFF(site_commissioned_on, LAG(site_commissioned_on) OVER (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST, id)) AS days_since_prev_site_commissioned_on,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY site_commissioned_on, id))) / 60, 1) AS minutes_since_prev_read_at,
        LEAD(kwh) OVER (ORDER BY site_commissioned_on, id) AS next_kwh,
        LEAD(site_commissioned_on) OVER w_all_by_site_commissioned_on AS next_site_commissioned_on,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST), 3) AS site_commissioned_on_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST, id) AS rn,
        ROUND(PERCENT_RANK() OVER w_all_by_voltage, 3) AS voltage_percent_rank,
        COUNT(*) OVER (PARTITION BY site_id, site_tariff) AS rows_in_site_id_site_tariff,
        ROUND(PERCENT_RANK() OVER w_site_id_by_site_commissioned, 3) AS site_commissioned_on_percent_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST), 3) AS site_commissioned_on_percent_rank_2,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id, site_tariff), 0), 4) AS voltage_share_of_site_id_site_tariff,
        ROUND(voltage - AVG(voltage) OVER w_site_id_site_tariff, 2) AS voltage_vs_site_id_site_tariff_avg,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id, site_tariff), 2) AS kwh_vs_site_id_site_tariff_avg
    FROM with_details_site
    WINDOW w_all_by_site_commissioned_on AS (ORDER BY site_commissioned_on, id), w_all_by_voltage AS (ORDER BY voltage DESC, id), w_site_id_by_site_commissioned AS (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST, id), w_site_id_site_tariff AS (PARTITION BY site_id, site_tariff)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        site_commissioned_on,
        site_tariff,
        site_name,
        days_since_prev_site_commissioned_on,
        next_kwh,
        site_commissioned_on_cume_dist,
        rn,
        rows_in_site_id_site_tariff,
        site_commissioned_on_percent_rank,
        site_commissioned_on_percent_rank_2,
        voltage_vs_site_id_site_tariff_avg,
        kwh_vs_site_id_site_tariff_avg,
        DENSE_RANK() OVER w_all_by_minutes_since_prev_re AS minutes_since_prev_read_at_dense_rank,
        ROUND(voltage - AVG(voltage) OVER w_rn_site_id, 2) AS voltage_vs_rn_site_id_avg,
        ROUND(AVG(next_kwh) OVER (PARTITION BY site_tariff ORDER BY read_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS next_kwh_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY site_tariff, DATE_TRUNC('MONTH', next_site_commissioned_on) ORDER BY rn ASC) AS rn_dense_rank,
        ROUND(voltage_percent_rank - AVG(voltage_percent_rank) OVER w_site_id, 2) AS voltage_percent_rank_vs_site_id_avg,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY rn, site_id)) AS distinct_site_tariff_in_rn_site_id
    FROM windowed
    WINDOW w_all_by_minutes_since_prev_re AS (ORDER BY minutes_since_prev_read_at DESC), w_rn_site_id AS (PARTITION BY rn, site_id), w_site_id AS (PARTITION BY site_id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        rn,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
        MIN(next_kwh) AS min_next_kwh,
        ROUND(SUM(next_kwh_avg_6_prior) / NULLIF(SUM(voltage_vs_rn_site_id_avg), 0), 3) AS next_kwh_avg_6_prior_to_voltage_vs_rn_site_id_av
    FROM metrics
    GROUP BY DATE_TRUNC('MONTH', site_commissioned_on), rn
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY rn) AS rows_in_rn,
        LAST_VALUE(median_kwh) OVER (ORDER BY next_kwh_avg_6_prior_to_voltage_vs_rn_site_id_av DESC, site_commissioned_on_month, rn ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_median_kwh,
        ROUND(CUME_DIST() OVER w_rn_by_min_next_kwh, 3) AS min_next_kwh_cume_dist
    FROM per_group
    WINDOW w_rn_by_min_next_kwh AS (PARTITION BY rn ORDER BY min_next_kwh, site_commissioned_on_month, rn)
)
-- Final projection
SELECT
    site_commissioned_on_month,
    rn,
    min_next_kwh,
    next_kwh_avg_6_prior_to_voltage_vs_rn_site_id_av,
    rows_in_rn,
    min_next_kwh_cume_dist,
    rn + median_kwh AS rn_plus_median_kwh,
    LEAST(min_next_kwh, rows_in_rn) AS least_min_next_kwh_rows_in_rn
FROM with_trends
WHERE median_kwh >= 18.0
ORDER BY min_next_kwh_cume_dist

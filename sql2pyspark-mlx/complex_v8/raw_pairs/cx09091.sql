WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh BETWEEN 24.5 AND 113.0
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_ref,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(voltage) OVER (ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4,
        DENSE_RANK() OVER (ORDER BY read_at DESC, id) AS read_at_dense_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY voltage DESC) AS voltage_rank,
        SIZE(COLLECT_SET(site_id) OVER w_site_tariff_site_commissione) AS distinct_site_id_in_site_tariff_site_commissione,
        ROUND(CUME_DIST() OVER w_all_by_read_at_2, 3) AS read_at_cume_dist,
        RANK() OVER (ORDER BY read_at NULLS LAST) AS read_at_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4_2,
        MIN(voltage) OVER w_site_commissioned_on_month AS site_commissioned_on_month_min_voltage,
        NTH_VALUE(voltage, 2) OVER (ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on,
        ROUND(CUME_DIST() OVER w_site_commissioned_on_month_b_4, 3) AS voltage_cume_dist,
        ROUND(AVG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY voltage DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_6_prior,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on)), 0), 4) AS voltage_share_of_site_commissioned_on_month,
        LAG(site_commissioned_on, 2) OVER (ORDER BY read_at DESC, id) AS prev2_site_commissioned_on,
        SUM(voltage) OVER w_site_tariff_site_commissione AS site_tariff_site_commissioned_on_month_sum_volta
    FROM enriched_site
    WINDOW w_site_tariff_site_commissione AS (PARTITION BY site_tariff, DATE_TRUNC('MONTH', site_commissioned_on)), w_all_by_read_at_2 AS (ORDER BY read_at NULLS LAST, id), w_site_commissioned_on_month AS (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on)), w_site_commissioned_on_month_b_4 AS (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY voltage DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE read_at_rank = 1
),
summary AS (
    -- Group-level totals
    SELECT
        site_id,
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        distinct_site_id_in_site_tariff_site_commissione,
        SUM(voltage_avg_4_2) AS total_voltage_avg_4_2,
        ROUND(AVG(second_voltage), 2) AS avg_second_voltage,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN site_tariff <> 'EV' THEN 1 END) / COUNT(*), 1) AS pct_site_tariff_ev,
        SUM(CASE WHEN site_tariff = 'TOU' THEN read_at_rank ELSE 0 END) AS read_at_rank_site_tariff_tou
    FROM top_per_group
    GROUP BY site_id, DATE_TRUNC('MONTH', site_commissioned_on), distinct_site_id_in_site_tariff_site_commissione
    HAVING MAX(voltage_avg_4_2) > 220.5
)
SELECT
    site_id,
    distinct_site_id_in_site_tariff_site_commissione,
    total_voltage_avg_4_2,
    pct_site_tariff_ev,
    read_at_rank_site_tariff_tou,
    total_voltage_avg_4_2 - cnt AS total_voltage_avg_4_2_minus_cnt
FROM summary
ORDER BY pct_site_tariff_ev, total_voltage_avg_4_2

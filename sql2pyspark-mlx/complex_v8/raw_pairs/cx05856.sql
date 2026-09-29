WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE (voltage > 223.0 OR site_id IS NOT NULL)
),
combined_details AS (
    -- Attach site attributes
    SELECT s.*, si.tariff AS site_tariff, si.site_name
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IN ('EV', 'TOU')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        quality,
        kwh_minus_voltage,
        site_tariff,
        site_name,
        SUM(kwh_minus_voltage) OVER (ORDER BY read_at ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_minus_voltage_sum_1_prior,
        ROUND(AVG(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        MIN(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality) AS read_at_month_quality_min_voltage,
        DENSE_RANK() OVER (ORDER BY read_at ASC NULLS LAST) AS read_at_dense_rank,
        LAST_VALUE(kwh_minus_voltage) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_minus_voltage,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY site_id, quality)) AS distinct_site_tariff_in_site_id_quality,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality)) AS distinct_site_tariff_in_read_at_month_quality,
        NTH_VALUE(kwh_minus_voltage, 2) OVER (PARTITION BY site_tariff ORDER BY kwh_minus_voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_minus_voltage,
        voltage - LAG(voltage) OVER w_site_tariff_by_kwh_minus_vol_2 AS voltage_change,
        LAST_VALUE(kwh_minus_voltage) OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh_minus_voltage,
        NTILE(10) OVER (PARTITION BY site_tariff ORDER BY read_at, id) AS read_at_decile,
        ROUND(kwh_minus_voltage - AVG(kwh_minus_voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality), 2) AS kwh_minus_voltage_vs_read_at_month_quality_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_tariff ORDER BY read_at), 3) AS read_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY site_id, quality ORDER BY read_at NULLS LAST) AS read_at_dense_rank_2,
        SUM(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        MIN(kwh_minus_voltage) OVER (PARTITION BY site_tariff ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_minus_voltage_so_far
    FROM combined_details
    WINDOW w_site_tariff_by_kwh_minus_vol_2 AS (PARTITION BY site_tariff ORDER BY kwh_minus_voltage NULLS LAST, id)
)
-- Final projection
SELECT DISTINCT read_at_percent_rank
FROM with_windows
ORDER BY read_at_percent_rank DESC

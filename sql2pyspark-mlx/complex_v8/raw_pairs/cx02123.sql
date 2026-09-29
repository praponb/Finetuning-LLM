WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 12:00:00'
      AND kwh <= 109.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY voltage_filled) AS voltage_filled_dense_rank,
        ROW_NUMBER() OVER w_site_id_quality_by_voltage AS rn,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY MONTH(read_at), site_id), 0), 4) AS kwh_share_of_read_at_month_site_id,
        ROUND(AVG(kwh_filled) OVER (ORDER BY voltage_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_filled_avg_3,
        voltage - LAG(voltage) OVER (ORDER BY voltage_filled, id) AS voltage_change,
        ROUND(AVG(kwh_filled) OVER (PARTITION BY site_id, quality ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_filled_avg_4,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        SUM(voltage) OVER (PARTITION BY site_id, quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        ROUND(AVG(voltage) OVER (ORDER BY voltage_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        ROUND(voltage - AVG(voltage) OVER w_site_id, 2) AS voltage_vs_site_id_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_kwh, 3) AS kwh_percent_rank,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage, id) AS voltage_rank,
        LEAD(kwh, 2) OVER w_all_by_voltage_filled_2 AS next2_kwh
    FROM cleaned_readings
    WINDOW w_site_id_quality_by_voltage AS (PARTITION BY site_id, quality ORDER BY voltage, id), w_site_id AS (PARTITION BY site_id), w_all_by_kwh AS (ORDER BY kwh, id), w_all_by_voltage_filled_2 AS (ORDER BY voltage_filled DESC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        voltage_filled,
        rn,
        voltage_change,
        voltage_vs_site_id_avg,
        voltage_rank,
        next2_kwh,
        DENSE_RANK() OVER w_quality_site_id_by_voltage_v AS voltage_vs_site_id_avg_dense_rank,
        next2_kwh - LAG(next2_kwh) OVER (PARTITION BY quality, site_id ORDER BY voltage_vs_site_id_avg ASC, id) AS next2_kwh_change,
        ROUND(CUME_DIST() OVER w_all_by_kwh_filled_avg_4, 3) AS kwh_filled_avg_4_cume_dist,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        FIRST_VALUE(kwh_filled) OVER (PARTITION BY site_id ORDER BY voltage_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_filled,
        LAG(voltage_filled, 3) OVER (ORDER BY kwh_filled_avg_4 NULLS LAST, id) AS prev3_voltage_filled
    FROM with_windows
    WINDOW w_quality_site_id_by_voltage_v AS (PARTITION BY quality, site_id ORDER BY voltage_vs_site_id_avg), w_all_by_kwh_filled_avg_4 AS (ORDER BY kwh_filled_avg_4 NULLS LAST, id)
)
SELECT
    id,
    site_id,
    kwh,
    voltage_filled,
    rn,
    voltage_change,
    voltage_vs_site_id_avg,
    voltage_rank,
    next2_kwh,
    voltage_vs_site_id_avg_dense_rank,
    next2_kwh_change,
    kwh_filled_avg_4_cume_dist,
    distinct_site_id_in_quality,
    first_kwh_filled,
    prev3_voltage_filled,
    UPPER(TRIM(quality)) AS quality_clean
FROM windowed
ORDER BY voltage_rank DESC

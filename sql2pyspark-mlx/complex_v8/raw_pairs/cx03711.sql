WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.07, 2) AS voltage_adj
    FROM meter_readings
    WHERE (voltage BETWEEN 218.2 AND 239.6 OR kwh < 82.9)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.voltage_adj,
        si.tariff AS site_tariff,
        si.site_name
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id AND si.commissioned_on < DATE '2025-11-28'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_quality_by_kwh, 3) AS kwh_percent_rank,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_tariff, site_id)) AS distinct_quality_in_site_tariff_site_id,
        COUNT(*) OVER (PARTITION BY site_tariff, site_id) AS rows_in_site_tariff_site_id,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        LAST_VALUE(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        ROUND(AVG(voltage_adj) OVER (PARTITION BY site_tariff, site_id), 2) AS site_tariff_site_id_avg_voltage_adj,
        LAG(read_at, 2) OVER w_all_by_kwh AS prev2_read_at,
        DENSE_RANK() OVER (ORDER BY kwh DESC) AS kwh_dense_rank,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY voltage_adj ASC NULLS LAST, id) AS voltage_adj_decile,
        RANK() OVER (PARTITION BY quality ORDER BY kwh ASC NULLS LAST, id) AS kwh_rank,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY kwh DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        LEAD(read_at) OVER (ORDER BY kwh DESC, id) AS next_read_at
    FROM combined_details
    WINDOW w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh ASC NULLS LAST, id), w_all_by_kwh AS (ORDER BY kwh DESC, id)
),
summary AS (
    -- Group-level totals
    SELECT
        TO_DATE(next_read_at) AS next_read_at_day,
        ROUND(AVG(distinct_quality_in_site_tariff_site_id), 2) AS avg_distinct_quality_in_site_tariff_site_id,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list,
        MAX(kwh_rank) AS max_kwh_rank,
        SUM(minutes_since_prev_read_at) AS total_minutes_since_prev_read_at
    FROM metrics
    GROUP BY TO_DATE(next_read_at)
)
-- Final projection
SELECT
    next_read_at_day,
    avg_distinct_quality_in_site_tariff_site_id,
    site_tariff_list,
    max_kwh_rank,
    total_minutes_since_prev_read_at,
    FLOOR(distinct_site_ids / 2) * 2 AS distinct_site_ids_bucket
FROM summary
WHERE SIZE(site_tariff_list) >= 1
ORDER BY max_kwh_rank NULLS LAST, total_minutes_since_prev_read_at DESC

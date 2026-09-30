WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE quality IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality_or_unknown), 2) AS read_at_month_quality_or_unknown_avg_voltage,
        SIZE(COLLECT_SET(quality_group) OVER (PARTITION BY quality, quality_or_unknown)) AS distinct_quality_group_in_quality_quality_or_unk,
        voltage - LAG(voltage) OVER (ORDER BY voltage, id) AS voltage_change,
        LEAD(voltage, 2) OVER (ORDER BY voltage, id) AS next2_voltage,
        ROUND(CUME_DIST() OVER w_quality_group_quality_by_vol, 3) AS voltage_cume_dist,
        SUM(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        LAG(voltage) OVER (PARTITION BY quality_group ORDER BY voltage DESC NULLS FIRST, id) AS prev_voltage,
        LAG(voltage, 2, 0) OVER (PARTITION BY quality, quality_group ORDER BY read_at DESC, id) AS prev2_voltage,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality_or_unknown)) AS distinct_quality_in_read_at_month_quality_or_unk
    FROM cleaned_readings
    WINDOW w_quality_group_quality_by_vol AS (PARTITION BY quality_group, quality ORDER BY voltage DESC, id), w_read_at_month_quality_or_unk AS (PARTITION BY DATE_TRUNC('MONTH', read_at), quality_or_unknown)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_or_unknown,
        read_at_month_quality_or_unknown_avg_voltage,
        distinct_quality_group_in_quality_quality_or_unk,
        voltage_change,
        running_voltage,
        prev_voltage,
        prev2_voltage,
        distinct_quality_in_read_at_month_quality_or_unk,
        ROUND(prev_voltage * 1.1, 2) AS prev_voltage_adj,
        LEAST(voltage_change, prev2_voltage) AS least_voltage_change_prev2_voltage
    FROM metrics
    WHERE running_voltage > 882.0
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    quality_or_unknown,
    read_at_month_quality_or_unknown_avg_voltage,
    distinct_quality_group_in_quality_quality_or_unk,
    running_voltage,
    prev_voltage,
    prev2_voltage,
    distinct_quality_in_read_at_month_quality_or_unk,
    prev_voltage_adj,
    least_voltage_change_prev2_voltage
FROM enriched
ORDER BY id DESC

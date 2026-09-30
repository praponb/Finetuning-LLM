WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 0.9, 2) AS voltage_adj,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        voltage_adj,
        quality_group,
        COALESCE(voltage_adj, 0.0) AS voltage_adj_filled
    FROM recent
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(voltage_adj_filled) OVER w_quality_quality_group AS quality_quality_group_max_voltage_adj_filled,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY quality_group, quality ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality_group, quality ORDER BY voltage DESC NULLS FIRST), 3) AS voltage_percent_rank
    FROM derived
    WINDOW w_quality_quality_group AS (PARTITION BY quality, quality_group)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        voltage,
        voltage_adj,
        quality_group,
        voltage_adj_filled,
        last_known_voltage,
        voltage_percent_rank,
        MIN(voltage_adj) OVER (PARTITION BY site_id, quality ORDER BY voltage_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_adj_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY voltage_adj ASC), 3) AS voltage_adj_cume_dist,
        ROUND(AVG(voltage_adj_filled) OVER (PARTITION BY site_id, quality ORDER BY voltage_adj ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_adj_filled_avg_2
    FROM with_windows
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY last_known_voltage DESC NULLS FIRST, id) AS last_known_voltage_rank,
        last_known_voltage - LAG(last_known_voltage) OVER (ORDER BY last_known_voltage DESC NULLS FIRST, id) AS last_known_voltage_change,
        voltage_adj_filled_avg_2 - LAG(voltage_adj_filled_avg_2) OVER (PARTITION BY site_id ORDER BY voltage, id) AS voltage_adj_filled_avg_2_change
    FROM ranked
)
SELECT
    id,
    voltage,
    voltage_adj,
    quality_group,
    last_known_voltage,
    min_voltage_adj_so_far,
    voltage_adj_cume_dist,
    last_known_voltage_change
FROM windowed
ORDER BY quality_group DESC, voltage_adj DESC NULLS FIRST

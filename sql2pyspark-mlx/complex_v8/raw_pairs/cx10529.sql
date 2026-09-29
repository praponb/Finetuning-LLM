WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 234.5
      AND site_id IS NOT NULL
),
totals AS (
    -- Group-level totals
    SELECT
        quality,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        MAX(voltage) AS max_voltage,
        SUM(voltage) AS total_voltage
    FROM prepared_readings
    WHERE site_id IS NOT NULL
    GROUP BY quality
    HAVING AVG(voltage) > 215.1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        quality,
        distinct_qualitys,
        max_voltage,
        ROUND(total_voltage - AVG(total_voltage) OVER w_distinct_qualitys, 2) AS total_voltage_vs_distinct_qualitys_avg,
        ROUND(AVG(total_voltage) OVER (PARTITION BY distinct_qualitys, quality ORDER BY max_voltage, quality ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS total_voltage_avg_3,
        ROW_NUMBER() OVER (PARTITION BY distinct_qualitys, quality ORDER BY max_voltage, quality) AS max_voltage_seq,
        SUM(total_voltage) OVER (PARTITION BY distinct_qualitys) AS distinct_qualitys_sum_total_voltage
    FROM totals
    WINDOW w_distinct_qualitys AS (PARTITION BY distinct_qualitys)
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE max_voltage_seq > (SELECT MIN(max_voltage_seq) FROM metrics)
),
leaders AS (
    -- Top 3 per group
    SELECT
        quality,
        distinct_qualitys,
        max_voltage,
        total_voltage_vs_distinct_qualitys_avg,
        total_voltage_avg_3,
        distinct_qualitys_sum_total_voltage
    FROM qualified
    WHERE max_voltage_seq <= 3
),
labeled AS (
    -- Derive helper columns
    SELECT
        quality,
        distinct_qualitys,
        total_voltage_vs_distinct_qualitys_avg,
        distinct_qualitys_sum_total_voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        ROUND(max_voltage / NULLIF(total_voltage_avg_3, 0), 3) AS max_voltage_per_total_voltage_avg_3,
        ROUND(total_voltage_avg_3 * 1.07, 2) AS total_voltage_avg_3_adj
    FROM leaders
)
-- Final projection
SELECT
    quality,
    distinct_qualitys,
    total_voltage_vs_distinct_qualitys_avg,
    max_voltage_per_total_voltage_avg_3,
    total_voltage_avg_3_adj,
    CONCAT_WS('-', quality, quality_or_unknown) AS quality_quality_or_unknown_label,
    ROUND(total_voltage_avg_3_adj / NULLIF(distinct_qualitys, 0), 3) AS total_voltage_avg_3_adj_per_distinct_qualitys
FROM labeled
ORDER BY total_voltage_avg_3_adj_per_distinct_qualitys DESC, quality
LIMIT 10

WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE (voltage BETWEEN 218.4 AND 238.8 OR kwh <= 110.5)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(read_at) OVER (PARTITION BY quality_or_unknown, quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY voltage, id), 3) AS voltage_cume_dist,
        SIZE(COLLECT_SET(quality_or_unknown) OVER (PARTITION BY quality, TO_DATE(read_at))) AS distinct_quality_or_unknown_in_quality_read_at_t,
        ROUND(AVG(voltage) OVER w_quality_or_unknown_quality, 2) AS quality_or_unknown_quality_avg_voltage,
        NTILE(4) OVER (ORDER BY voltage DESC, id) AS voltage_quartile,
        SIZE(COLLECT_SET(site_id) OVER w_quality_or_unknown_quality) AS distinct_site_id_in_quality_or_unknown_quality,
        DENSE_RANK() OVER w_quality_or_unknown_quality_b_2 AS voltage_dense_rank,
        ROUND(AVG(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_2,
        MAX(voltage) OVER (PARTITION BY quality, TO_DATE(read_at)) AS quality_read_at_to_date_max_voltage,
        SUM(voltage) OVER (PARTITION BY quality_or_unknown, quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        MIN(voltage) OVER (PARTITION BY quality, quality_or_unknown) AS quality_quality_or_unknown_min_voltage,
        MAX(voltage) OVER w_quality_or_unknown_quality AS quality_or_unknown_quality_max_voltage
    FROM filtered
    WINDOW w_quality_or_unknown_quality AS (PARTITION BY quality_or_unknown, quality), w_quality_or_unknown_quality_b_2 AS (PARTITION BY quality_or_unknown, quality ORDER BY voltage)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(voltage) OVER w_quality_or_unknown AS quality_or_unknown_sum_voltage,
        LAG(voltage_avg_2, 3, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', first_read_at) ORDER BY voltage DESC, id) AS prev3_voltage_avg_2,
        LAG(voltage_cume_dist, 3) OVER (PARTITION BY voltage_dense_rank, TO_DATE(first_read_at) ORDER BY running_voltage DESC, id) AS prev3_voltage_cume_dist,
        MIN(voltage_dense_rank) OVER w_quality_or_unknown AS quality_or_unknown_min_voltage_dense_rank,
        ROUND(voltage_cume_dist - AVG(voltage_cume_dist) OVER (PARTITION BY quality_or_unknown), 2) AS voltage_cume_dist_vs_quality_or_unknown_avg
    FROM with_trends
    WINDOW w_quality_or_unknown AS (PARTITION BY quality_or_unknown)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(first_read_at, 'yyyy-MM') AS first_read_at_ym,
        CASE WHEN voltage >= 236.7 THEN 'A' WHEN voltage >= 224.8 THEN 'B' ELSE 'C' END AS voltage_tier,
        MAX(read_at) AS last_read_at,
        ROUND(SUM(prev3_voltage_cume_dist) / NULLIF(SUM(distinct_quality_or_unknown_in_quality_read_at_t), 0), 3) AS prev3_voltage_cume_dist_to_distinct_quality_or_u,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS n_quality_bad,
        SUM(voltage_dense_rank) AS total_voltage_dense_rank
    FROM ranked
    WHERE running_voltage BETWEEN 770.0 AND 1666.0
    GROUP BY DATE_FORMAT(first_read_at, 'yyyy-MM'), CASE WHEN voltage >= 236.7 THEN 'A' WHEN voltage >= 224.8 THEN 'B' ELSE 'C' END
)
-- Final projection
SELECT
    first_read_at_ym,
    voltage_tier,
    last_read_at,
    prev3_voltage_cume_dist_to_distinct_quality_or_u,
    n_quality_bad,
    total_voltage_dense_rank,
    DATE_TRUNC('WEEK', last_read_at) AS last_read_at_week,
    CONCAT_WS(': ', first_read_at_ym, voltage_tier) AS first_read_at_ym_voltage_tier_label
FROM per_group
ORDER BY total_voltage_dense_rank DESC

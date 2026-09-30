WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_group,
        si.commissioned_on AS site_commissioned_on
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        voltage,
        quality,
        quality_group,
        ROW_NUMBER() OVER (ORDER BY voltage, id) AS seq_no,
        MAX(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        FIRST_VALUE(site_commissioned_on) OVER (ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_commissioned_on,
        MIN(voltage) OVER (PARTITION BY quality) AS quality_min_voltage,
        MAX(voltage) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on)) AS site_id_site_commissioned_on_month_max_voltage,
        ROW_NUMBER() OVER (ORDER BY site_commissioned_on, id) AS site_commissioned_on_seq,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on))) AS distinct_quality_in_site_id_site_commissioned_on,
        LAG(site_commissioned_on) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY site_commissioned_on DESC, id) AS prev_site_commissioned_on,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on)), 2) AS site_id_site_commissioned_on_month_avg_voltage,
        SUM(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_sum_2_prior
    FROM joined_site
    WINDOW w_site_commissioned_on_month_q AS (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY site_commissioned_on DESC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        voltage,
        quality,
        quality_group,
        seq_no,
        max_voltage_so_far,
        first_site_commissioned_on,
        quality_min_voltage,
        site_id_site_commissioned_on_month_max_voltage,
        distinct_quality_in_site_id_site_commissioned_on,
        prev_site_commissioned_on,
        site_id_site_commissioned_on_month_avg_voltage,
        voltage_sum_2_prior
    FROM with_trends
    WHERE site_commissioned_on_seq <= 5
      AND prev_site_commissioned_on < DATE '2025-05-07'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        seq_no,
        max_voltage_so_far,
        first_site_commissioned_on,
        quality_min_voltage,
        distinct_quality_in_site_id_site_commissioned_on,
        site_id_site_commissioned_on_month_avg_voltage,
        FIRST_VALUE(voltage_sum_2_prior) IGNORE NULLS OVER (PARTITION BY distinct_quality_in_site_id_site_commissioned_on ORDER BY first_site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_sum_2_prior,
        COUNT(*) OVER (PARTITION BY seq_no, distinct_quality_in_site_id_site_commissioned_on) AS rows_in_seq_no_distinct_quality_in_site_id_site_,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY seq_no, distinct_quality_in_site_id_site_commissioned_on), 2) AS voltage_vs_seq_no_distinct_quality_in_site_id_si,
        ROUND(quality_min_voltage - AVG(quality_min_voltage) OVER (PARTITION BY seq_no, distinct_quality_in_site_id_site_commissioned_on), 2) AS quality_min_voltage_vs_seq_no_distinct_quality_i,
        ROUND(AVG(voltage_sum_2_prior) OVER (ORDER BY quality_min_voltage DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_sum_2_prior_avg_7
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    max_voltage_so_far,
    first_site_commissioned_on,
    quality_min_voltage,
    distinct_quality_in_site_id_site_commissioned_on,
    first_known_voltage_sum_2_prior,
    rows_in_seq_no_distinct_quality_in_site_id_site_,
    voltage_vs_seq_no_distinct_quality_in_site_id_si,
    quality_min_voltage_vs_seq_no_distinct_quality_i,
    voltage_sum_2_prior_avg_7
FROM ranked
ORDER BY id
LIMIT 10

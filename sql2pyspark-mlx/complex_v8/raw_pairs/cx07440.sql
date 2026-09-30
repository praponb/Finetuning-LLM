WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.1, 2) AS kwh_adj
    FROM meter_readings
    WHERE kwh >= 34.6
      AND voltage >= 218.2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        voltage,
        quality,
        kwh_adj,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at NULLS LAST), 3) AS read_at_percent_rank,
        NTH_VALUE(read_at, 2) OVER (ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        LAG(voltage, 3, 0) OVER w_all_by_read_at AS prev3_voltage,
        SUM(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_sum_2_prior,
        COUNT(*) OVER w_quality_site_id AS rows_in_quality_site_id,
        NTILE(4) OVER (ORDER BY voltage, id) AS voltage_quartile
    FROM cleaned_readings
    WINDOW w_all_by_read_at AS (ORDER BY read_at NULLS LAST, id), w_quality_site_id AS (PARTITION BY quality, site_id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(voltage_sum_2_prior / NULLIF(SUM(voltage_sum_2_prior) OVER (PARTITION BY voltage_quartile, DATE_TRUNC('MONTH', second_read_at)), 0), 4) AS voltage_sum_2_prior_share_of_voltage_quartile_se,
        DENSE_RANK() OVER (ORDER BY second_read_at) AS second_read_at_dense_rank,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY voltage_quartile, DATE_TRUNC('MONTH', second_read_at)), 2) AS voltage_vs_voltage_quartile_second_read_at_month,
        LEAD(read_at_percent_rank, 2, 0) OVER w_all_by_second_read_at AS next2_read_at_percent_rank
    FROM sequenced
    WINDOW w_all_by_second_read_at AS (ORDER BY second_read_at, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        ROUND(voltage_sum_2_prior * 0.9, 2) AS voltage_sum_2_prior_adj
    FROM sequenced_2
    WHERE TO_DATE(read_at) < DATE '2025-02-18'
),
sequenced_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY second_read_at_dense_rank) AS rows_in_second_read_at_dense_rank,
        SIZE(COLLECT_SET(quality) OVER w_second_read_at_dense_rank) AS distinct_quality_in_second_read_at_dense_rank,
        ROUND(AVG(second_read_at_dense_rank) OVER (ORDER BY voltage_vs_voltage_quartile_second_read_at_month DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS second_read_at_dense_rank_avg_6_prior
    FROM enriched
    WINDOW w_second_read_at_dense_rank AS (PARTITION BY second_read_at_dense_rank)
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    quality,
    read_at_percent_rank,
    second_read_at,
    voltage_sum_2_prior,
    voltage_quartile,
    voltage_sum_2_prior_share_of_voltage_quartile_se,
    second_read_at_dense_rank,
    voltage_vs_voltage_quartile_second_read_at_month,
    next2_read_at_percent_rank,
    quality_prefix,
    voltage_sum_2_prior_adj,
    distinct_quality_in_second_read_at_dense_rank,
    second_read_at_dense_rank_avg_6_prior,
    DATE_FORMAT(second_read_at, 'yyyy-MM') AS second_read_at_label,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM sequenced_3
ORDER BY voltage_vs_voltage_quartile_second_read_at_month DESC, quality_prefix DESC NULLS FIRST

WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        ROUND(kwh * 0.9, 2) AS kwh_adj
    FROM meter_readings
    WHERE kwh BETWEEN 14.5 AND 81.9
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS read_at_month_sum_kwh,
        ROUND(kwh_adj - AVG(kwh_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)), 2) AS kwh_adj_vs_read_at_month_avg,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id, quality ORDER BY kwh_adj DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(PERCENT_RANK() OVER w_site_id_quality_by_kwh_adj, 3) AS kwh_adj_percent_rank,
        kwh_adj - LAG(kwh_adj) OVER w_site_id_by_kwh AS kwh_adj_change,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS rows_in_read_at_month,
        DENSE_RANK() OVER w_site_id_quality_by_kwh_adj_2 AS kwh_adj_dense_rank,
        NTILE(5) OVER (PARTITION BY site_id ORDER BY kwh_adj ASC, id) AS kwh_adj_quintile,
        LAG(kwh_adj) OVER (PARTITION BY site_id ORDER BY kwh, id) AS prev_kwh_adj,
        DENSE_RANK() OVER w_site_id_by_kwh_ties AS kwh_dense_rank,
        MIN(kwh_adj) OVER (PARTITION BY site_id, quality) AS site_id_quality_min_kwh_adj,
        SUM(kwh_per_voltage) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_per_voltage_sum_2_prior,
        LEAD(read_at) OVER (PARTITION BY site_id ORDER BY kwh, id) AS next_read_at
    FROM scoped
    WINDOW w_site_id_quality_by_kwh_adj AS (PARTITION BY site_id, quality ORDER BY kwh_adj DESC NULLS FIRST, id), w_site_id_by_kwh AS (PARTITION BY site_id ORDER BY kwh, id), w_site_id_quality_by_kwh_adj_2 AS (PARTITION BY site_id, quality ORDER BY kwh_adj ASC, id), w_site_id_by_kwh_ties AS (PARTITION BY site_id ORDER BY kwh)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh_per_voltage,
        kwh_adj,
        read_at_month_sum_kwh,
        kwh_adj_vs_read_at_month_avg,
        second_read_at,
        kwh_adj_percent_rank,
        kwh_adj_change,
        rows_in_read_at_month,
        kwh_adj_quintile,
        prev_kwh_adj,
        kwh_dense_rank,
        site_id_quality_min_kwh_adj,
        kwh_per_voltage_sum_2_prior,
        next_read_at
    FROM with_trends
    WHERE kwh_adj_dense_rank <= 2
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh_per_voltage,
        read_at_month_sum_kwh,
        kwh_adj_vs_read_at_month_avg,
        second_read_at,
        kwh_adj_percent_rank,
        kwh_adj_change,
        rows_in_read_at_month,
        kwh_adj_quintile,
        prev_kwh_adj,
        kwh_dense_rank,
        site_id_quality_min_kwh_adj,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY kwh_dense_rank)) AS distinct_quality_in_kwh_dense_rank,
        MAX(kwh_adj_percent_rank) OVER (PARTITION BY kwh_dense_rank) AS kwh_dense_rank_max_kwh_adj_percent_rank,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh_per_voltage_sum_2_prior NULLS LAST, id) AS kwh_per_voltage_sum_2_prior_dense_rank,
        prev_kwh_adj - LAG(prev_kwh_adj) OVER (PARTITION BY kwh_dense_rank, YEAR(second_read_at) ORDER BY kwh_dense_rank DESC NULLS FIRST, id) AS prev_kwh_adj_change,
        ROUND(PERCENT_RANK() OVER w_read_at_to_date_by_kwh_per_v, 3) AS kwh_per_voltage_sum_2_prior_percent_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY kwh_dense_rank)) AS distinct_site_id_in_kwh_dense_rank,
        ROUND(CUME_DIST() OVER w_read_at_to_date_by_kwh_per_v_2, 3) AS kwh_per_voltage_sum_2_prior_cume_dist
    FROM top_per_group
    WINDOW w_read_at_to_date_by_kwh_per_v AS (PARTITION BY TO_DATE(read_at) ORDER BY kwh_per_voltage_sum_2_prior NULLS LAST, id), w_read_at_to_date_by_kwh_per_v_2 AS (PARTITION BY TO_DATE(read_at) ORDER BY kwh_per_voltage_sum_2_prior NULLS LAST)
),
stats AS (
    -- Group-level totals
    SELECT
        CASE WHEN prev_kwh_adj_change >= 126.0 THEN 'A' WHEN prev_kwh_adj_change >= -72.0 THEN 'B' ELSE 'C' END AS prev_kwh_adj_change_tier,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        SUM(CASE WHEN quality = 'BAD' THEN prev_kwh_adj_change ELSE 0 END) AS prev_kwh_adj_change_quality_bad,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        MAX(second_read_at) AS last_second_read_at
    FROM with_trends_2
    GROUP BY 1, 2
    HAVING MAX(prev_kwh_adj_change) > -126.0
)
-- Final projection
SELECT
    prev_kwh_adj_change_tier,
    read_at_month,
    distinct_site_ids,
    last_second_read_at,
    COALESCE(distinct_site_ids, 0) AS distinct_site_ids_filled,
    DATEDIFF(DATE '2026-01-01', last_second_read_at) AS days_since_last_second_read_at
FROM stats
ORDER BY read_at_month DESC NULLS FIRST, prev_kwh_adj_change_tier
LIMIT 20

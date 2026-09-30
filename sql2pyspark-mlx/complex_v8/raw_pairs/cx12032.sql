WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 14:00:00'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality), 0), 4) AS voltage_share_of_quality,
        ROUND(voltage - AVG(voltage) OVER w_quality, 2) AS voltage_vs_quality_avg,
        MIN(kwh) OVER w_quality AS quality_min_kwh,
        COUNT(*) OVER w_quality AS rows_in_quality,
        SUM(voltage) OVER (PARTITION BY quality) AS quality_sum_voltage,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST), 3) AS kwh_cume_dist,
        SUM(kwh) OVER (PARTITION BY quality) AS quality_sum_kwh,
        ROUND(kwh - AVG(kwh) OVER w_quality, 2) AS kwh_vs_quality_avg,
        NTILE(10) OVER (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST, id) AS kwh_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY voltage), 3) AS voltage_percent_rank
    FROM filtered
    WINDOW w_quality AS (PARTITION BY quality)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', quality, read_at_label) AS quality_read_at_label_label,
        ROUND(rows_in_quality / NULLIF(quality_min_kwh, 0), 3) AS rows_in_quality_per_quality_min_kwh
    FROM with_trends
    WHERE kwh_vs_quality_avg > -48.0
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_kwh_decile_by_kwh_ties AS kwh_rank,
        NTILE(4) OVER (PARTITION BY site_id ORDER BY quality_sum_kwh, id) AS quality_sum_kwh_quartile,
        ROW_NUMBER() OVER w_quality_by_rows_in_quality AS rows_in_quality_seq,
        LAST_VALUE(voltage_share_of_quality) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY quality_sum_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_share_of_quality,
        LAG(kwh_cume_dist) OVER (PARTITION BY site_id ORDER BY quality_sum_kwh, id) AS prev_kwh_cume_dist,
        MIN(kwh_decile) OVER (PARTITION BY kwh_decile) AS kwh_decile_min_kwh_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY kwh_decile ORDER BY kwh DESC), 3) AS kwh_percent_rank
    FROM labeled
    WINDOW w_kwh_decile_by_kwh_ties AS (PARTITION BY kwh_decile ORDER BY kwh DESC), w_quality_by_rows_in_quality AS (PARTITION BY quality ORDER BY rows_in_quality NULLS LAST, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        rows_in_quality_seq,
        site_id,
        SUM(rows_in_quality_per_quality_min_kwh) AS total_rows_in_quality_per_quality_min_kwh,
        ROUND(SUM(kwh_cume_dist) / NULLIF(SUM(kwh), 0), 3) AS kwh_cume_dist_to_kwh_ratio,
        SUM(CASE WHEN quality IN ('BAD', 'EST') THEN 1 ELSE 0 END) AS n_quality_in_bad_est,
        MAX(kwh) AS max_kwh
    FROM with_trends_2
    WHERE kwh_decile_min_kwh_decile <= 55
    GROUP BY rows_in_quality_seq, site_id
),
with_trends_3 AS (
    -- Rank and compare rows within groups
    SELECT
        rows_in_quality_seq,
        site_id,
        total_rows_in_quality_per_quality_min_kwh,
        n_quality_in_bad_est,
        max_kwh,
        max_kwh - LAG(max_kwh) OVER (ORDER BY kwh_cume_dist_to_kwh_ratio DESC, rows_in_quality_seq, site_id) AS max_kwh_change,
        ROUND(max_kwh / NULLIF(SUM(max_kwh) OVER (PARTITION BY n_quality_in_bad_est), 0), 4) AS max_kwh_share_of_n_quality_in_bad_est,
        MAX(kwh_cume_dist_to_kwh_ratio) OVER (PARTITION BY n_quality_in_bad_est) AS n_quality_in_bad_est_max_kwh_cume_dist_to_kwh_ra
    FROM grouped
)
-- Final projection
SELECT
    rows_in_quality_seq,
    site_id,
    total_rows_in_quality_per_quality_min_kwh,
    n_quality_in_bad_est,
    n_quality_in_bad_est_max_kwh_cume_dist_to_kwh_ra,
    ROUND(max_kwh * 0.9, 2) AS max_kwh_adj
FROM with_trends_3
WHERE total_rows_in_quality_per_quality_min_kwh > 12.0
ORDER BY n_quality_in_bad_est DESC, total_rows_in_quality_per_quality_min_kwh

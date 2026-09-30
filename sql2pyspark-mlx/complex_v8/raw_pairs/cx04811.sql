WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'BAD')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(voltage) OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY voltage) AS voltage_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id) AS read_at_rank,
        COUNT(*) OVER w_read_at_month AS rows_in_read_at_month,
        LAG(voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id) AS prev_voltage,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        ROUND(PERCENT_RANK() OVER w_quality_site_id_by_read_at_t, 3) AS read_at_percent_rank,
        MIN(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS read_at_month_min_voltage,
        MAX(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS read_at_month_max_voltage
    FROM base
    WINDOW w_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', read_at)), w_quality_site_id_by_read_at_t AS (PARTITION BY quality, site_id ORDER BY read_at DESC)
),
enriched AS (
    -- Label and score rows
    SELECT *, COALESCE(voltage_avg_3_prior, 0.0) AS voltage_avg_3_prior_filled
    FROM windowed
    WHERE rows_in_read_at_month BETWEEN 4 AND 19
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(running_voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at_rank DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS running_voltage_avg_2_prior,
        COUNT(*) OVER (PARTITION BY read_at_rank, quality) AS rows_in_read_at_rank_quality,
        ROW_NUMBER() OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at, id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at), 3) AS read_at_cume_dist_2,
        ROUND(AVG(rows_in_read_at_month) OVER (PARTITION BY read_at_rank, quality), 2) AS read_at_rank_quality_avg_rows_in_read_at_month
    FROM enriched
),
grouped AS (
    -- Summarise per group
    SELECT
        seq_no,
        ROUND(SUM(read_at_rank_quality_avg_rows_in_read_at_month) / NULLIF(SUM(rows_in_read_at_month), 0), 3) AS read_at_rank_quality_avg_rows_in_read_at_month_t,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        ROUND(SUM(voltage_avg_3_prior) / NULLIF(SUM(read_at_rank_quality_avg_rows_in_read_at_month), 0), 3) AS voltage_avg_3_prior_to_read_at_rank_quality_avg_,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM metrics
    GROUP BY 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_rank_quality, 3) AS read_at_rank_quality_avg_rows_in_read_at_month_t_2,
        ROUND(distinct_site_ids - AVG(distinct_site_ids) OVER (PARTITION BY distinct_site_ids, seq_no), 2) AS distinct_site_ids_vs_distinct_site_ids_seq_no_av,
        NTILE(5) OVER (ORDER BY read_at_rank_quality_avg_rows_in_read_at_month_t DESC, seq_no) AS read_at_rank_quality_avg_rows_in_read_at_month_t_3
    FROM grouped
    WINDOW w_all_by_read_at_rank_quality AS (ORDER BY read_at_rank_quality_avg_rows_in_read_at_month_t DESC)
)
SELECT
    seq_no,
    read_at_rank_quality_avg_rows_in_read_at_month_t,
    read_at_rank_quality_avg_rows_in_read_at_month_t_2,
    read_at_rank_quality_avg_rows_in_read_at_month_t_3,
    LEAST(seq_no, distinct_site_ids) AS least_seq_no_distinct_site_ids,
    ABS(seq_no - 5) AS seq_no_distance
FROM with_windows

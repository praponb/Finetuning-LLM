WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DATE_TRUNC('YEAR', start_date) AS start_date_year,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE product NOT IN ('HEALTH', 'AUTO')
      AND start_date IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_holder_id_start_date_month_b AS rn,
        ROUND(PERCENT_RANK() OVER w_holder_id_start_date_month_b_2, 3) AS days_start_date_to_end_date_percent_rank
    FROM filtered
    WINDOW w_holder_id_start_date_month_b AS (PARTITION BY holder_id, MONTH(start_date) ORDER BY days_start_date_to_end_date DESC, id), w_holder_id_start_date_month_b_2 AS (PARTITION BY holder_id, MONTH(start_date) ORDER BY days_start_date_to_end_date DESC)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)) AS rows_in_start_date_month,
        NTH_VALUE(days_start_date_to_end_date, 2) OVER (PARTITION BY holder_id ORDER BY start_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_start_date_to_end_date
    FROM ranked
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(CASE WHEN product NOT IN ('AUTO', 'HEALTH') THEN 1 END) AS policy_product_count
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_product_count, 0) AS policy_product_count
    FROM with_windows s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        premium,
        status,
        start_date_year,
        days_start_date_to_end_date,
        rn,
        rows_in_start_date_month,
        second_days_start_date_to_end_date,
        NTILE(5) OVER (PARTITION BY product, YEAR(end_date) ORDER BY premium DESC, id) AS premium_quintile,
        ROUND(AVG(policy_product_count) OVER (PARTITION BY product, YEAR(end_date) ORDER BY premium DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS policy_product_count_avg_3_prior,
        SIZE(COLLECT_SET(status) OVER w_product_end_date_month) AS distinct_status_in_product_end_date_month
    FROM summarized_rows
    WINDOW w_product_end_date_month AS (PARTITION BY product, MONTH(end_date))
),
status_stats AS (
    -- Median days_start_date_to_end_date per status
    SELECT status, PERCENTILE_APPROX(days_start_date_to_end_date, 0.5) AS median_days_start_date_to_end_date
    FROM metrics
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_start_date_to_end_date,
        ROUND(s.days_start_date_to_end_date - g.median_days_start_date_to_end_date, 2) AS days_start_date_to_end_date_minus_median
    FROM metrics s
    JOIN status_stats g ON g.status = s.status
    WHERE s.days_start_date_to_end_date >= g.median_days_start_date_to_end_date
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        premium,
        status,
        days_start_date_to_end_date,
        rn,
        rows_in_start_date_month,
        second_days_start_date_to_end_date,
        distinct_status_in_product_end_date_month,
        median_days_start_date_to_end_date,
        days_start_date_to_end_date_minus_median,
        ROUND(PERCENT_RANK() OVER w_all_by_premium_quintile, 3) AS premium_quintile_percent_rank,
        ROUND(premium_quintile - AVG(premium_quintile) OVER (PARTITION BY distinct_status_in_product_end_date_month), 2) AS premium_quintile_vs_distinct_status_in_product_e,
        RANK() OVER (ORDER BY premium_quintile, id) AS premium_quintile_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY policy_product_count_avg_3_prior DESC, id), 3) AS policy_product_count_avg_3_prior_percent_rank,
        COUNT(*) OVER (PARTITION BY distinct_status_in_product_end_date_month) AS rows_in_distinct_status_in_product_end_date_mont
    FROM above_average
    WINDOW w_all_by_premium_quintile AS (ORDER BY premium_quintile, id)
)
-- Final projection
SELECT
    id,
    product,
    days_start_date_to_end_date,
    rn,
    distinct_status_in_product_end_date_month,
    median_days_start_date_to_end_date,
    days_start_date_to_end_date_minus_median,
    premium_quintile_percent_rank,
    premium_quintile_vs_distinct_status_in_product_e,
    premium_quintile_rank,
    policy_product_count_avg_3_prior_percent_rank,
    rows_in_distinct_status_in_product_end_date_mont
FROM ranked_2
ORDER BY rn DESC

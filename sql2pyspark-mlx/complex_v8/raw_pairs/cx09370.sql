WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE holder_id <= 14
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.status_slug,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM filtered_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        holder_joined_on,
        holder_region,
        holder_risk_score,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY start_date ASC, id) AS start_date_dense_rank,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_sum_1_prior,
        LAG(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id) AS prev_start_date,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_status, 0), 4) AS holder_risk_score_share_of_status,
        MAX(premium) OVER (PARTITION BY holder_region, DATE_TRUNC('MONTH', end_date)) AS holder_region_end_date_month_max_premium,
        LEAD(end_date, 3) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id) AS next3_end_date,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC) AS end_date_rank,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY holder_region, DATE_TRUNC('MONTH', end_date))) AS distinct_holder_id_in_holder_region_end_date_mon,
        NTH_VALUE(start_date, 2) OVER (ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        SIZE(COLLECT_SET(holder_region) OVER w_status) AS distinct_holder_region_in_status,
        SIZE(COLLECT_SET(status) OVER w_end_date_year) AS distinct_status_in_end_date_year,
        LEAD(premium) OVER (ORDER BY holder_risk_score, id) AS next_premium,
        COUNT(*) OVER (PARTITION BY YEAR(end_date)) AS rows_in_end_date_year
    FROM joined_holder
    WINDOW w_status AS (PARTITION BY status), w_end_date_year AS (PARTITION BY YEAR(end_date))
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        start_date,
        status,
        holder_joined_on,
        holder_risk_score,
        start_date_dense_rank,
        premium_sum_1_prior,
        end_date_rank,
        distinct_holder_id_in_holder_region_end_date_mon,
        second_start_date,
        distinct_status_in_end_date_year,
        next_premium,
        DENSE_RANK() OVER w_second_start_date_month_by_p AS premium_dense_rank,
        LAST_VALUE(end_date) OVER (PARTITION BY MONTH(end_date), MONTH(holder_joined_on) ORDER BY distinct_status_in_end_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date
    FROM windowed
    WINDOW w_second_start_date_month_by_p AS (PARTITION BY DATE_TRUNC('MONTH', second_start_date) ORDER BY premium)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        DATE_TRUNC('MONTH', second_start_date) AS second_start_date_month,
        product,
        ROUND(SUM(end_date_rank) / NULLIF(SUM(holder_risk_score), 0), 3) AS end_date_rank_to_holder_risk_score_ratio,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        PERCENTILE_APPROX(distinct_holder_id_in_holder_region_end_date_mon, 0.5) AS median_distinct_holder_id_in_holder_region_end_d,
        ROUND(AVG(end_date_rank), 2) AS avg_end_date_rank
    FROM ranked
    GROUP BY status, DATE_TRUNC('MONTH', second_start_date), product
    HAVING MAX(next_premium) > 2033.8
)
-- Final projection
SELECT
    status,
    product,
    end_date_rank_to_holder_risk_score_ratio,
    product_list,
    median_distinct_holder_id_in_holder_region_end_d,
    avg_end_date_rank,
    ROUND(avg_end_date_rank * 0.25, 2) AS avg_end_date_rank_adj,
    ROUND(avg_end_date_rank * 1.1, 2) AS avg_end_date_rank_adj_2
FROM rollup_base
ORDER BY end_date_rank_to_holder_risk_score_ratio DESC NULLS FIRST, median_distinct_holder_id_in_holder_region_end_d

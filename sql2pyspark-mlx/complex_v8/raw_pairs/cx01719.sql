WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status)) AS distinct_product_in_status,
        LAG(premium) OVER (ORDER BY start_date DESC, id) AS prev_premium
    FROM prepared
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        distinct_product_in_status,
        ROUND(AVG(premium), 2) AS avg_premium,
        ROUND(AVG(prev_premium), 2) AS avg_prev_premium,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM ranked
    GROUP BY product, distinct_product_in_status
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(distinct_product_in_status) OVER (ORDER BY avg_prev_premium DESC, product, distinct_product_in_status ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS distinct_product_in_status_max_2_prior,
        SUM(distinct_product_in_status) OVER (ORDER BY avg_prev_premium DESC, product, distinct_product_in_status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_product_in_status,
        RANK() OVER w_all_by_avg_prev_premium_ties AS avg_prev_premium_rank,
        MIN(distinct_product_in_status) OVER (PARTITION BY distinct_product_in_status, product) AS distinct_product_in_status_product_min_distinct_,
        ROUND(distinct_product_in_status - AVG(distinct_product_in_status) OVER (PARTITION BY distinct_product_in_status, product), 2) AS distinct_product_in_status_vs_distinct_product_i
    FROM rollup_base
    WINDOW w_all_by_avg_prev_premium_ties AS (ORDER BY avg_prev_premium DESC)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(product)) AS product_len,
        ROUND(avg_prev_premium * 1.07, 2) AS avg_prev_premium_adj,
        ROUND(avg_prev_premium / NULLIF(avg_prev_premium_rank, 0), 3) AS avg_prev_premium_per_avg_prev_premium_rank
    FROM windowed
    WHERE SIZE(product_list) >= 1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE avg_premium > (SELECT AVG(avg_premium) FROM derived)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM qualified
    WHERE product_len > (SELECT MIN(product_len) FROM qualified)
)
-- Final projection
SELECT
    product,
    distinct_product_in_status,
    avg_premium,
    avg_prev_premium,
    product_list,
    distinct_product_in_status_max_2_prior,
    running_distinct_product_in_status,
    distinct_product_in_status_product_min_distinct_,
    distinct_product_in_status_vs_distinct_product_i,
    product_len,
    avg_prev_premium_per_avg_prev_premium_rank,
    SUBSTRING(TRIM(product), 1, 3) AS product_prefix
FROM with_activity
ORDER BY avg_prev_premium_per_avg_prev_premium_rank DESC

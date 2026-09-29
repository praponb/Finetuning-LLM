WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        CASE WHEN product IN ('HEALTH', 'AUTO') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE status = 'CANCELLED'
      AND start_date >= DATE '2024-11-20'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_2_prior,
        RANK() OVER w_holder_id_by_premium AS premium_rank,
        COUNT(*) OVER (PARTITION BY product_group, status ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        RANK() OVER (PARTITION BY product_group, status ORDER BY premium) AS premium_rank_2,
        SIZE(COLLECT_SET(product_group) OVER (PARTITION BY status)) AS distinct_product_group_in_status,
        ROUND(AVG(premium) OVER (PARTITION BY product_group, product), 2) AS product_group_product_avg_premium,
        RANK() OVER (ORDER BY premium DESC, id) AS premium_rank_3
    FROM prepared
    WINDOW w_holder_id_by_premium AS (PARTITION BY holder_id ORDER BY premium, id)
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY premium_rank_2 ORDER BY premium_avg_2_prior DESC), 3) AS premium_avg_2_prior_percent_rank,
        MIN(premium_rank) OVER (PARTITION BY status) AS status_min_premium_rank,
        ROUND(CUME_DIST() OVER (ORDER BY running_count, id), 3) AS running_count_cume_dist,
        SUM(premium_rank_3) OVER (ORDER BY running_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_rank_3,
        LAG(premium, 2, 0) OVER (ORDER BY running_count, id) AS prev2_premium
    FROM metrics
)
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    premium_avg_2_prior,
    premium_rank,
    premium_share_of_status,
    premium_rank_2,
    distinct_product_group_in_status,
    product_group_product_avg_premium,
    premium_rank_3,
    premium_avg_2_prior_percent_rank,
    status_min_premium_rank,
    running_count_cume_dist,
    running_premium_rank_3,
    prev2_premium,
    LEAST(premium_rank_2, premium_rank) AS least_premium_rank_2_premium_rank
FROM metrics_2
ORDER BY status_min_premium_rank DESC, premium_rank_3

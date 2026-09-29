WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        ABS(premium - 2319.8) AS premium_distance
    FROM policies
    WHERE (holder_id >= 8 OR product = 'LIFE')
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.premium_adj,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM cleaned_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium_adj - AVG(premium_adj) OVER w_holder_joined_on_month, 2) AS premium_adj_vs_holder_joined_on_month_avg,
        NTH_VALUE(holder_joined_on, 2) OVER (PARTITION BY product, holder_region ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_joined_on,
        MAX(premium) OVER (PARTITION BY status, holder_id ORDER BY holder_joined_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_max_6_prior,
        MAX(premium_adj) OVER (PARTITION BY product, holder_segment ORDER BY premium_adj, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_adj_max_3_prior,
        COUNT(*) OVER w_holder_joined_on_month AS rows_in_holder_joined_on_month,
        NTILE(3) OVER (PARTITION BY status, holder_id ORDER BY holder_joined_on, id) AS holder_joined_on_tercile,
        MAX(premium) OVER (PARTITION BY product, holder_region ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_max_6_prior_2,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER w_holder_joined_on_month, 0), 4) AS premium_adj_share_of_holder_joined_on_month,
        LAG(premium, 2) OVER w_product_holder_segment_by_pr_2 AS prev2_premium,
        LAG(holder_joined_on, 3) OVER (PARTITION BY product, holder_segment ORDER BY premium_adj, id) AS prev3_holder_joined_on,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on)), 0), 4) AS premium_share_of_holder_joined_on_month
    FROM with_details
    WINDOW w_holder_joined_on_month AS (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on)), w_product_holder_segment_by_pr_2 AS (PARTITION BY product, holder_segment ORDER BY premium_adj, id)
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY YEAR(prev3_holder_joined_on), holder_joined_on_tercile ORDER BY rows_in_holder_joined_on_month DESC) AS rows_in_holder_joined_on_month_dense_rank,
        LAST_VALUE(premium_adj) OVER (ORDER BY premium_adj ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_adj,
        LEAD(second_holder_joined_on, 2) OVER (PARTITION BY YEAR(prev3_holder_joined_on), holder_joined_on_tercile ORDER BY rows_in_holder_joined_on_month DESC, id) AS next2_second_holder_joined_on,
        ROUND(PERCENT_RANK() OVER w_prev3_holder_joined_on_year_2, 3) AS rows_in_holder_joined_on_month_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(prev3_holder_joined_on), holder_joined_on_tercile ORDER BY rows_in_holder_joined_on_month DESC), 3) AS rows_in_holder_joined_on_month_percent_rank_2,
        SIZE(COLLECT_SET(holder_id) OVER w_holder_joined_on_month_holde) AS distinct_holder_id_in_holder_joined_on_month_hol,
        MAX(premium_max_6_prior) OVER w_holder_joined_on_month_holde AS holder_joined_on_month_holder_region_max_premium
    FROM windowed
    WINDOW w_prev3_holder_joined_on_year_2 AS (PARTITION BY YEAR(prev3_holder_joined_on), holder_joined_on_tercile ORDER BY rows_in_holder_joined_on_month DESC, id), w_holder_joined_on_month_holde AS (PARTITION BY MONTH(holder_joined_on), holder_region)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(next2_second_holder_joined_on) AS next2_second_holder_joined_on_year,
        SUM(rows_in_holder_joined_on_month_dense_rank) AS total_rows_in_holder_joined_on_month_dense_rank,
        ROUND(SUM(premium_adj) / NULLIF(SUM(distinct_holder_id_in_holder_joined_on_month_hol), 0), 3) AS premium_adj_to_distinct_holder_id_in_holder_join,
        MIN(next2_second_holder_joined_on) AS first_next2_second_holder_joined_on,
        SUM(CASE WHEN product IN ('LIFE', 'HOME') THEN 1 ELSE 0 END) AS n_product_in_life_home,
        SUM(holder_joined_on_month_holder_region_max_premium) FILTER (WHERE product = 'HOME') AS holder_joined_on_month_holder_region_max_premium_2
    FROM windowed_2
    GROUP BY YEAR(next2_second_holder_joined_on)
    HAVING SUM(rows_in_holder_joined_on_month_percent_rank) > 0.1
)
-- Final projection
SELECT
    next2_second_holder_joined_on_year,
    total_rows_in_holder_joined_on_month_dense_rank,
    premium_adj_to_distinct_holder_id_in_holder_join,
    first_next2_second_holder_joined_on,
    holder_joined_on_month_holder_region_max_premium_2
FROM rollup_base
WHERE premium_adj_to_distinct_holder_id_in_holder_join > 1.0
ORDER BY premium_adj_to_distinct_holder_id_in_holder_join DESC NULLS FIRST, next2_second_holder_joined_on_year
LIMIT 10

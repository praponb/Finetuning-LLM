WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE product = 'LIFE'
      AND holder_id IS NOT NULL
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        p.joined_on AS holder_joined_on
    FROM filtered_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        start_date,
        holder_joined_on,
        ROUND(premium - AVG(premium) OVER w_start_date_year_product, 2) AS premium_vs_start_date_year_product_avg,
        ROUND(premium - AVG(premium) OVER w_holder_id_holder_joined_on_m, 2) AS premium_vs_holder_id_holder_joined_on_month_avg,
        SUM(premium) OVER (ORDER BY holder_joined_on DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        DENSE_RANK() OVER (ORDER BY holder_joined_on DESC) AS holder_joined_on_dense_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER w_start_date_year_product, 0), 4) AS premium_share_of_start_date_year_product,
        LEAD(start_date) OVER (ORDER BY start_date, id) AS next_start_date,
        ROUND(CUME_DIST() OVER w_holder_joined_on_month_by_st, 3) AS start_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY holder_joined_on DESC), 3) AS holder_joined_on_percent_rank,
        premium - LAG(premium) OVER (PARTITION BY MONTH(holder_joined_on) ORDER BY start_date, id) AS premium_change,
        RANK() OVER w_product_by_premium_ties AS premium_rank,
        LEAD(start_date, 2) OVER w_product_by_premium AS next2_start_date,
        MAX(premium) OVER (PARTITION BY YEAR(start_date)) AS start_date_year_max_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY product ORDER BY premium), 3) AS premium_cume_dist,
        SIZE(COLLECT_SET(product) OVER w_holder_id_holder_joined_on_m) AS distinct_product_in_holder_id_holder_joined_on_m
    FROM combined_details
    WINDOW w_start_date_year_product AS (PARTITION BY YEAR(start_date), product), w_holder_id_holder_joined_on_m AS (PARTITION BY holder_id, MONTH(holder_joined_on)), w_holder_joined_on_month_by_st AS (PARTITION BY MONTH(holder_joined_on) ORDER BY start_date), w_product_by_premium_ties AS (PARTITION BY product ORDER BY premium), w_product_by_premium AS (PARTITION BY product ORDER BY premium, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        holder_id,
        product,
        start_date,
        holder_joined_on,
        premium_vs_start_date_year_product_avg,
        premium_vs_holder_id_holder_joined_on_month_avg,
        premium_sum_3_prior,
        holder_joined_on_dense_rank,
        premium_share_of_start_date_year_product,
        next_start_date,
        start_date_cume_dist,
        holder_joined_on_percent_rank,
        premium_change,
        next2_start_date,
        start_date_year_max_premium,
        premium_cume_dist,
        distinct_product_in_holder_id_holder_joined_on_m
    FROM windowed
    WHERE premium_rank <= 5
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    holder_joined_on,
    premium_vs_start_date_year_product_avg,
    premium_vs_holder_id_holder_joined_on_month_avg,
    premium_share_of_start_date_year_product,
    next_start_date,
    start_date_cume_dist,
    holder_joined_on_percent_rank,
    premium_change,
    next2_start_date,
    start_date_year_max_premium,
    premium_cume_dist,
    distinct_product_in_holder_id_holder_joined_on_m
FROM top_per_group
ORDER BY premium_vs_holder_id_holder_joined_on_month_avg, holder_joined_on_percent_rank

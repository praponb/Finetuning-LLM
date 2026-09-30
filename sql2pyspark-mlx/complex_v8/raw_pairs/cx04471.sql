WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        p.joined_on AS holder_joined_on
    FROM base s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_product_by_premium AS seq_no,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER (PARTITION BY product ORDER BY premium, id)) AS days_since_prev_holder_joined_on,
        NTILE(10) OVER (PARTITION BY product ORDER BY premium, id) AS premium_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(holder_joined_on), holder_id ORDER BY premium), 3) AS premium_cume_dist,
        MIN(premium) OVER (PARTITION BY product) AS product_min_premium,
        LAG(premium) OVER (PARTITION BY YEAR(holder_joined_on), holder_id ORDER BY premium, id) AS prev_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        ROUND(premium - AVG(premium) OVER w_product, 2) AS premium_vs_product_avg,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER (PARTITION BY YEAR(holder_joined_on), holder_id ORDER BY premium, id)) AS days_since_prev_holder_joined_on_2,
        FIRST_VALUE(holder_joined_on) IGNORE NULLS OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_joined_on,
        SUM(premium) OVER (ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        RANK() OVER w_product_by_premium_ties AS premium_rank
    FROM enriched_holder
    WINDOW w_product_by_premium AS (PARTITION BY product ORDER BY premium, id), w_product AS (PARTITION BY product), w_product_by_premium_ties AS (PARTITION BY product ORDER BY premium)
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY premium_rank ORDER BY seq_no DESC), 3) AS seq_no_cume_dist,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        NTH_VALUE(premium_decile, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_holder_joined_on) ORDER BY premium_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_decile,
        SUM(premium_decile) OVER (PARTITION BY premium_rank ORDER BY seq_no DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_decile_sum_1_prior,
        ROUND(CUME_DIST() OVER w_first_known_holder_joined_on_2, 3) AS premium_cume_dist_cume_dist,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_holder_joined_on) ORDER BY premium_cume_dist, id) AS premium_cume_dist_rank,
        ROUND(AVG(product_min_premium) OVER w_holder_id, 2) AS holder_id_avg_product_min_premium,
        LAG(premium_share_of_product) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_holder_joined_on) ORDER BY premium_cume_dist, id) AS prev_premium_share_of_product
    FROM windowed
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_first_known_holder_joined_on_2 AS (PARTITION BY DATE_TRUNC('MONTH', first_known_holder_joined_on) ORDER BY premium_cume_dist, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        second_premium_decile,
        holder_id,
        YEAR(first_known_holder_joined_on) AS first_known_holder_joined_on_year,
        COUNT(DISTINCT product) AS distinct_products,
        MIN(rows_in_holder_id) AS min_rows_in_holder_id,
        COUNT(*) AS n_rows,
        ROUND(AVG(premium_cume_dist_cume_dist), 2) AS avg_premium_cume_dist_cume_dist,
        ROUND(STDDEV(premium_decile), 2) AS stddev_premium_decile
    FROM windowed_2
    WHERE days_since_prev_holder_joined_on BETWEEN 60 AND 130
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
)
SELECT
    second_premium_decile,
    holder_id,
    first_known_holder_joined_on_year,
    distinct_products,
    min_rows_in_holder_id,
    n_rows,
    avg_premium_cume_dist_cume_dist,
    stddev_premium_decile,
    LEAST(distinct_products, n_rows) AS least_distinct_products_n_rows,
    ABS(second_premium_decile - 6) AS second_premium_decile_distance
FROM rollup_base
ORDER BY second_premium_decile, first_known_holder_joined_on_year NULLS LAST

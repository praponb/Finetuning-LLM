WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claimed_amount BETWEEN 7204.1 AND 33690.0
              AND c.adjuster_id IS NOT NULL
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium, id), 3) AS premium_percent_rank,
        ROW_NUMBER() OVER (ORDER BY premium, id) AS rn,
        FIRST_VALUE(product_len) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_product_len,
        ROUND(product_len / NULLIF(SUM(product_len) OVER (PARTITION BY MONTH(start_date)), 0), 4) AS product_len_share_of_start_date_month,
        FIRST_VALUE(premium) IGNORE NULLS OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        NTILE(10) OVER (ORDER BY premium, id) AS premium_decile
    FROM without_matches
),
product_stats AS (
    -- Max rn per product
    SELECT product, MAX(rn) AS max_rn
    FROM metrics
    GROUP BY product
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_rn, ROUND(s.rn - g.max_rn, 2) AS rn_minus_max
    FROM metrics s
    JOIN product_stats g ON g.product = s.product
    WHERE s.rn = g.max_rn
)
SELECT
    premium_decile,
    first_product_len,
    GROUPING(premium_decile) AS is_total_premium_decile,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list
FROM vs_group
GROUP BY CUBE(premium_decile, first_product_len)
ORDER BY premium_decile NULLS LAST, first_product_len NULLS LAST

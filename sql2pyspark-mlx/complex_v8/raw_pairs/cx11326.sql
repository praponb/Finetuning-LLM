WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY product) AS product_sum_premium,
        ROUND(premium - AVG(premium) OVER w_product, 2) AS premium_vs_product_avg,
        ROW_NUMBER() OVER (ORDER BY end_date DESC NULLS FIRST, id) AS seq_no,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product)) AS distinct_holder_id_in_product,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product, 0), 4) AS premium_share_of_product,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        RANK() OVER w_product_by_start_date AS start_date_rank
    FROM valid_policys
    WINDOW w_product AS (PARTITION BY product), w_product_by_start_date AS (PARTITION BY product ORDER BY start_date DESC, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, seq_no ORDER BY start_date, id), 3) AS start_date_percent_rank,
        ROUND(premium_share_of_product - AVG(premium_share_of_product) OVER (PARTITION BY seq_no, start_date_rank), 2) AS premium_share_of_product_vs_seq_no_start_date_ra,
        NTH_VALUE(rows_in_product, 2) OVER (PARTITION BY product, seq_no ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_product,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY seq_no, start_date_rank)) AS distinct_status_in_seq_no_start_date_rank,
        MAX(rows_in_product) OVER w_seq_no_start_date_rank AS seq_no_start_date_rank_max_rows_in_product,
        NTILE(5) OVER (ORDER BY premium ASC, id) AS premium_quintile,
        premium - LAG(premium) OVER (ORDER BY premium ASC, id) AS premium_change,
        RANK() OVER (ORDER BY premium ASC) AS premium_rank
    FROM metrics
    WINDOW w_seq_no_start_date_rank AS (PARTITION BY seq_no, start_date_rank)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        premium_quintile,
        status,
        ROUND(SUM(distinct_holder_id_in_product) / NULLIF(SUM(start_date_percent_rank), 0), 3) AS distinct_holder_id_in_product_to_start_date_perc,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM with_windows
    GROUP BY product, premium_quintile, status
    HAVING SUM(distinct_status_in_seq_no_start_date_rank) > 4
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        premium_quintile - LAG(premium_quintile) OVER w_product_status_by_distinct_h AS premium_quintile_change,
        LAST_VALUE(premium_quintile) IGNORE NULLS OVER (PARTITION BY product, status ORDER BY distinct_holder_id_in_product_to_start_date_perc DESC, product, premium_quintile, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_quintile
    FROM summary
    WINDOW w_product_status_by_distinct_h AS (PARTITION BY product, status ORDER BY distinct_holder_id_in_product_to_start_date_perc DESC, product, premium_quintile, status)
)
-- Final projection
SELECT
    product,
    premium_quintile,
    status,
    distinct_holder_id_in_product_to_start_date_perc,
    premium_quintile_change,
    last_known_premium_quintile
FROM metrics_2

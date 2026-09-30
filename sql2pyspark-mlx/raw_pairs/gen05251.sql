WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        DATEDIFF(CURRENT_DATE(), sale_date) / 30 AS months_since_sale_date
    FROM store_sales
    WHERE sale_date > DATE '2023-04-17'
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        b.price,
        b.category
    FROM cleaned e
    INNER JOIN products b
        ON e.product_id = b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(category) IGNORE NULLS OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_category,
        SUM(price) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price,
        ROUND(price / SUM(price) OVER (PARTITION BY category, store_id), 4) AS price_share,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS seq_no,
        SUM(price) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price
    FROM joined
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        price,
        seq_no,
        MAX(seq_no) OVER w_part AS running_max_seq_no,
        SUM(seq_no) OVER w_part AS running_sum_seq_no,
        COUNT(seq_no) OVER w_part AS running_count_seq_no,
        LAG(price, 1) OVER w_grp AS prev_price
    FROM enriched
    WINDOW w_part AS (PARTITION BY product_id ORDER BY cumulative_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY seq_no, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, price + running_sum_seq_no AS price_plus_running_sum_seq_no
    FROM metrics
    WHERE running_sum_seq_no < 39
),
metrics_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(price) OVER (ORDER BY prev_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS price_avg_2_prior,
        DENSE_RANK() OVER (ORDER BY running_count_seq_no DESC NULLS LAST) AS running_count_seq_no_dense_rank,
        AVG(running_sum_seq_no) OVER (ORDER BY running_sum_seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_sum_seq_no_avg_3,
        PERCENT_RANK() OVER (ORDER BY running_count_seq_no NULLS LAST) AS running_count_seq_no_percent_rank
    FROM valid
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_price >= 1290 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_price_tier,
    APPROX_PERCENTILE(running_count_seq_no, ARRAY(0.25, 0.5, 0.75)) AS running_count_seq_no_quartiles,
    COUNT(*) AS row_count
FROM metrics_store_sales
GROUP BY CASE WHEN prev_price >= 1290 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY row_count

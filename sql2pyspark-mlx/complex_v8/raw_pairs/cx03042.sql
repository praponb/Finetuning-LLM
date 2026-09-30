WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        QUARTER(start_date) AS start_date_quarter
    FROM policies
    WHERE start_date >= DATE '2024-12-03'
      AND status IN ('CANCELLED', 'LAPSED', 'ACTIVE')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        end_date,
        NTILE(10) OVER (PARTITION BY start_date_quarter, holder_id ORDER BY start_date_quarter DESC, id) AS start_date_quarter_decile,
        MAX(start_date_quarter) OVER w_start_date_quarter_end_date AS start_date_quarter_end_date_month_max_start_date,
        start_date_quarter - LAG(start_date_quarter) OVER (PARTITION BY product, holder_id ORDER BY premium ASC, id) AS start_date_quarter_change,
        MAX(start_date_quarter) OVER w_product AS product_max_start_date_quarter,
        RANK() OVER (PARTITION BY start_date_quarter, holder_id ORDER BY start_date_quarter DESC, id) AS start_date_quarter_rank,
        ROUND(AVG(premium) OVER (PARTITION BY product), 2) AS product_avg_premium,
        DENSE_RANK() OVER w_product_holder_id_by_premium AS premium_dense_rank,
        DENSE_RANK() OVER w_start_date_quarter_holder_id_2 AS start_date_quarter_dense_rank,
        MIN(premium) OVER w_start_date_quarter_product AS start_date_quarter_product_min_premium
    FROM filtered_policys
    WINDOW w_start_date_quarter_end_date AS (PARTITION BY start_date_quarter, DATE_TRUNC('MONTH', end_date)), w_product AS (PARTITION BY product), w_product_holder_id_by_premium AS (PARTITION BY product, holder_id ORDER BY premium ASC, id), w_start_date_quarter_holder_id_2 AS (PARTITION BY start_date_quarter, holder_id ORDER BY start_date_quarter DESC), w_start_date_quarter_product AS (PARTITION BY start_date_quarter, product)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(product_avg_premium) IGNORE NULLS OVER (PARTITION BY premium_dense_rank ORDER BY product_avg_premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_avg_premium,
        MAX(start_date_quarter_decile) OVER (PARTITION BY premium_dense_rank) AS premium_dense_rank_max_start_date_quarter_decile,
        ROUND(start_date_quarter_rank / NULLIF(SUM(start_date_quarter_rank) OVER (PARTITION BY premium_dense_rank), 0), 4) AS start_date_quarter_rank_share_of_premium_dense_r
    FROM windowed
)
SELECT
    id,
    product,
    end_date,
    start_date_quarter_end_date_month_max_start_date,
    start_date_quarter_change,
    product_max_start_date_quarter,
    product_avg_premium,
    premium_dense_rank,
    start_date_quarter_product_min_premium,
    premium_dense_rank_max_start_date_quarter_decile,
    start_date_quarter_rank_share_of_premium_dense_r
FROM with_windows

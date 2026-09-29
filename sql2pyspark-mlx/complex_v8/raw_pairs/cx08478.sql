WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date >= DATE '2026-01-24'
),
unique_rows AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY end_date DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_product_by_premium_ties AS premium_dense_rank,
        ROW_NUMBER() OVER w_product_by_premium AS seq_no,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY product ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        MAX(premium) OVER w_product_holder_id AS product_holder_id_max_premium
    FROM unique_rows
    WINDOW w_product_by_premium_ties AS (PARTITION BY product ORDER BY premium ASC), w_product_by_premium AS (PARTITION BY product ORDER BY premium ASC, id), w_product_holder_id AS (PARTITION BY product, holder_id)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    status,
    premium_dense_rank,
    seq_no,
    second_start_date,
    product_holder_id_max_premium,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM with_trends
ORDER BY id

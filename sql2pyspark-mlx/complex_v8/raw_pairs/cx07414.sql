WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        TRUNC(start_date, 'MM') AS start_date_month_start,
        INITCAP(TRIM(product)) AS product_clean
    FROM policies
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_month_start,
        s.product_clean,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY end_date, id), 3) AS end_date_percent_rank,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY end_date, id) AS end_date_dense_rank
    FROM enriched
),
latest AS (
    -- Keep the latest row per holder_id, product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        start_date_month_start,
        product_clean,
        holder_name,
        holder_joined_on,
        end_date_percent_rank,
        end_date_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY end_date DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
with_details AS (
    -- Join policyholders
    SELECT s.*, p.segment AS holder_segment, p.holder_name AS holder_holder_name
    FROM latest s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    start_date_month_start,
    product_clean,
    holder_name,
    holder_segment,
    holder_holder_name,
    ROUND(end_date_percent_rank * 1.5, 2) AS end_date_percent_rank_adj
FROM with_details
WHERE UPPER(TRIM(holder_holder_name)) LIKE 'D%'
ORDER BY end_date

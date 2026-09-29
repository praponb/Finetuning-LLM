WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        MONTH(end_date) AS end_date_month,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        start_date,
        end_date,
        end_date_month,
        premium_bucket,
        COUNT(*) OVER (PARTITION BY holder_id, product) AS rows_in_holder_id_product,
        ROW_NUMBER() OVER (PARTITION BY end_date_month, status ORDER BY end_date DESC, id) AS seq_no,
        SUM(end_date_month) OVER (PARTITION BY holder_id, product) AS holder_id_product_sum_end_date_month
    FROM recent_policys
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        product,
        start_date,
        end_date,
        end_date_month,
        premium_bucket,
        rows_in_holder_id_product,
        holder_id_product_sum_end_date_month
    FROM with_windows
    WHERE seq_no <= 3
)
SELECT id, field, reading
FROM (
    SELECT id, holder_id_product_sum_end_date_month, premium_bucket, end_date_month, rows_in_holder_id_product
    FROM top_rows
) src
UNPIVOT (
    reading FOR field IN (holder_id_product_sum_end_date_month, premium_bucket, end_date_month, rows_in_holder_id_product)
) unp
ORDER BY id, field

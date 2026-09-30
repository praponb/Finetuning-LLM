WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE (status IS NOT NULL OR premium >= 1042.5)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status, product ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        RANK() OVER (PARTITION BY status, product ORDER BY start_date, id) AS start_date_rank,
        ROUND(CUME_DIST() OVER w_status_product_by_start_date_3, 3) AS start_date_cume_dist,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status, product ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        DENSE_RANK() OVER w_status_product_by_start_date_3 AS start_date_dense_rank
    FROM recent_policys
    WINDOW w_status_product_by_start_date_3 AS (PARTITION BY status, product ORDER BY start_date)
),
deduped AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        second_start_date,
        start_date_rank,
        start_date_cume_dist,
        first_known_start_date,
        start_date_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY start_date_dense_rank DESC, id DESC) AS recency_rank
        FROM with_windows
    ) ranked
    WHERE recency_rank = 1
),
unique_rows AS (
    -- Keep the latest row per product, holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        second_start_date,
        start_date_rank,
        start_date_cume_dist,
        first_known_start_date,
        start_date_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY start_date DESC, id DESC) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    start_date,
    status,
    second_start_date,
    start_date_rank,
    start_date_cume_dist,
    first_known_start_date,
    start_date_dense_rank,
    DATEDIFF(second_start_date, first_known_start_date) AS days_first_known_start_date_to_second_start_date,
    ROUND(start_date_cume_dist * 0.25, 2) AS start_date_cume_dist_adj
FROM unique_rows
WHERE holder_id IS NOT NULL
ORDER BY first_known_start_date DESC NULLS FIRST, start_date_cume_dist_adj DESC NULLS FIRST

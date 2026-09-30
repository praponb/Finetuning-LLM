WITH scoped_policys AS (
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
    WHERE (status NOT IN ('CANCELLED', 'LAPSED') OR premium < 3465.4)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        SUM(premium) OVER (ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_sum_4,
        LEAD(start_date, 3) OVER (ORDER BY end_date DESC NULLS FIRST, id) AS next3_start_date,
        MIN(premium) OVER w_holder_id AS holder_id_min_premium,
        DENSE_RANK() OVER w_all_by_end_date_ties AS end_date_dense_rank,
        FIRST_VALUE(end_date) OVER (ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date
    FROM scoped_policys
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_all_by_end_date_ties AS (ORDER BY end_date DESC NULLS FIRST)
),
tagged AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM ranked
),
deduped AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        premium_vs_holder_id_avg,
        premium_sum_4,
        next3_start_date,
        holder_id_min_premium,
        end_date_dense_rank,
        first_end_date,
        product_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium DESC, id DESC) AS row_num
        FROM tagged
    ) ranked
    WHERE row_num = 1
)
SELECT id, metric, value
FROM (
    SELECT id, premium_vs_holder_id_avg, premium
    FROM deduped
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (premium_vs_holder_id_avg AS premium_vs_holder_id_avg_v, premium AS m_premium)
) unp
ORDER BY id, metric

WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE start_date >= DATE '2024-07-19'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY product, holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        NTH_VALUE(premium, 2) OVER (PARTITION BY product, holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        RANK() OVER w_product_holder_id_by_end_dat_2 AS end_date_rank,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date
    FROM base_policys
    WINDOW w_product_holder_id_by_end_dat_2 AS (PARTITION BY product, holder_id ORDER BY end_date, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        second_end_date,
        second_premium,
        first_known_end_date
    FROM with_trends
    WHERE end_date_rank <= 5
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.second_end_date,
        s.second_premium,
        s.first_known_end_date,
        p.segment AS holder_segment
    FROM first_rows s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(holder_segment) OVER w_holder_id_end_date_month) AS distinct_holder_segment_in_holder_id_end_date_mo,
        DENSE_RANK() OVER w_end_date_month_holder_id_by AS first_known_end_date_dense_rank,
        COUNT(*) OVER (PARTITION BY holder_id, MONTH(end_date)) AS rows_in_holder_id_end_date_month,
        LEAD(end_date) OVER (PARTITION BY MONTH(end_date), holder_segment ORDER BY second_premium NULLS LAST, id) AS next_end_date
    FROM combined_details_holder
    WINDOW w_holder_id_end_date_month AS (PARTITION BY holder_id, MONTH(end_date)), w_end_date_month_holder_id_by AS (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id ORDER BY first_known_end_date ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    end_date,
    second_end_date,
    second_premium,
    distinct_holder_segment_in_holder_id_end_date_mo,
    first_known_end_date_dense_rank,
    rows_in_holder_id_end_date_month,
    CONCAT(holder_segment, '#', CAST(id AS STRING)) AS holder_segment_ref,
    DATEDIFF(DATE '2025-06-30', second_end_date) AS days_since_second_end_date
FROM windowed
ORDER BY distinct_holder_segment_in_holder_id_end_date_mo NULLS LAST

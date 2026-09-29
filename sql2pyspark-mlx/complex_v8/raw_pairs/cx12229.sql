WITH cleaned_policys AS (
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
    WHERE holder_id <= 17
      AND product = 'LIFE'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        p.segment AS holder_segment
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment NOT IN ('SME', 'VIP')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        MAX(premium) OVER w_holder_id AS holder_id_max_premium,
        NTILE(3) OVER (ORDER BY start_date, id) AS start_date_tercile,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        COUNT(*) OVER (PARTITION BY holder_id, holder_segment ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, MONTH(start_date)), 2) AS premium_vs_holder_id_start_date_month_avg,
        LAG(start_date) OVER w_product_holder_segment_by_st AS prev_start_date,
        LAG(start_date, 3) OVER (PARTITION BY holder_id, holder_segment ORDER BY premium, id) AS prev3_start_date,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id, holder_segment ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        DENSE_RANK() OVER w_holder_segment_by_start_date AS start_date_dense_rank,
        SUM(premium) OVER w_holder_segment AS holder_segment_sum_premium,
        LAST_VALUE(start_date) OVER (PARTITION BY holder_id, holder_segment ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER w_holder_id_holder_segment_by_2) AS days_since_prev_start_date,
        COUNT(*) OVER (PARTITION BY holder_segment) AS rows_in_holder_segment,
        COUNT(*) OVER (PARTITION BY holder_id, MONTH(start_date)) AS rows_in_holder_id_start_date_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, holder_segment ORDER BY start_date, id), 3) AS start_date_percent_rank
    FROM joined
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_product_holder_segment_by_st AS (PARTITION BY product, holder_segment ORDER BY start_date, id), w_holder_segment_by_start_date AS (PARTITION BY holder_segment ORDER BY start_date ASC, id), w_holder_segment AS (PARTITION BY holder_segment), w_holder_id_holder_segment_by_2 AS (PARTITION BY holder_id, holder_segment ORDER BY premium, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        holder_id_max_premium,
        start_date_tercile,
        premium_vs_holder_id_avg,
        running_count,
        premium_vs_holder_id_start_date_month_avg,
        prev_start_date,
        prev3_start_date,
        first_known_premium,
        holder_segment_sum_premium,
        last_start_date,
        days_since_prev_start_date,
        rows_in_holder_segment,
        rows_in_holder_id_start_date_month,
        start_date_percent_rank
    FROM with_windows
    WHERE start_date_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    holder_id_max_premium,
    running_count,
    premium_vs_holder_id_start_date_month_avg,
    prev_start_date,
    prev3_start_date,
    holder_segment_sum_premium,
    last_start_date,
    rows_in_holder_segment,
    rows_in_holder_id_start_date_month,
    start_date_percent_rank,
    ABS(rows_in_holder_segment - 10) AS rows_in_holder_segment_distance
FROM top_per_group
ORDER BY rows_in_holder_id_start_date_month DESC NULLS FIRST

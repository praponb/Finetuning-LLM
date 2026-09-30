WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM policies
    WHERE premium < 3465.4
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_adj,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id AND YEAR(p.joined_on) = 2024
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        premium_adj,
        NTILE(5) OVER (PARTITION BY holder_region, product ORDER BY start_date ASC, id) AS start_date_quintile,
        ROUND(AVG(premium) OVER (PARTITION BY holder_region, product ORDER BY start_date ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        SUM(premium_adj) OVER (PARTITION BY holder_region, product ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_adj,
        RANK() OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY end_date DESC) AS end_date_rank,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_segment ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        premium_adj - LAG(premium_adj) OVER (PARTITION BY holder_segment ORDER BY start_date, id) AS premium_adj_change,
        DENSE_RANK() OVER (PARTITION BY status, holder_id ORDER BY premium_adj) AS premium_adj_dense_rank,
        LEAD(premium, 2) OVER w_holder_region_product_by_sta AS next2_premium,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY status, holder_id ORDER BY premium_adj, id)) AS days_since_prev_end_date
    FROM with_details_holder
    WINDOW w_holder_region_product_by_sta AS (PARTITION BY holder_region, product ORDER BY start_date ASC, id)
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(second_end_date) OVER (PARTITION BY premium_adj_dense_rank ORDER BY days_since_prev_end_date ASC, id) AS next_second_end_date,
        ROUND(PERCENT_RANK() OVER w_premium_adj_dense_rank_by_da_2, 3) AS days_since_prev_end_date_percent_rank,
        COUNT(*) OVER w_end_date_rank_holder_id AS rows_in_end_date_rank_holder_id
    FROM with_windows
    WINDOW w_premium_adj_dense_rank_by_da_2 AS (PARTITION BY premium_adj_dense_rank ORDER BY days_since_prev_end_date ASC), w_end_date_rank_holder_id AS (PARTITION BY end_date_rank, holder_id)
)
-- Final projection
SELECT
    id,
    holder_id,
    end_date,
    status,
    premium_adj,
    premium_avg_1_prior,
    running_premium_adj,
    next2_premium,
    days_since_prev_end_date,
    next_second_end_date,
    days_since_prev_end_date_percent_rank,
    rows_in_end_date_rank_holder_id,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
    ABS(start_date_quintile - 6) AS start_date_quintile_distance
FROM with_windows_2
ORDER BY start_date_quintile_distance DESC

WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date
    FROM policies
    WHERE product IN ('HEALTH', 'AUTO')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(days_since_end_date) OVER (PARTITION BY status ORDER BY end_date DESC, id) AS prev_days_since_end_date,
        MIN(days_since_end_date) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_end_date_so_far,
        ROUND(days_since_end_date / NULLIF(SUM(days_since_end_date) OVER (PARTITION BY product, holder_id), 0), 4) AS days_since_end_date_share_of_product_holder_id,
        LAST_VALUE(days_since_end_date) IGNORE NULLS OVER (PARTITION BY product ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_end_date,
        DATEDIFF(end_date, LAG(end_date) OVER (ORDER BY end_date DESC, id)) AS days_since_prev_end_date,
        SUM(days_since_end_date) OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_end_date_sum_4,
        LAG(days_since_end_date, 3, 0) OVER w_product_by_end_date AS prev3_days_since_end_date,
        ROUND(PERCENT_RANK() OVER w_all_by_end_date, 3) AS end_date_percent_rank
    FROM base_policys
    WINDOW w_product_by_end_date AS (PARTITION BY product ORDER BY end_date ASC, id), w_all_by_end_date AS (ORDER BY end_date DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        end_date,
        days_since_end_date_share_of_product_holder_id,
        last_known_days_since_end_date,
        days_since_prev_end_date,
        days_since_end_date_sum_4,
        end_date_percent_rank,
        MAX(prev3_days_since_end_date) OVER (PARTITION BY status) AS status_max_prev3_days_since_end_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(end_date), status ORDER BY prev3_days_since_end_date ASC NULLS LAST), 3) AS prev3_days_since_end_date_percent_rank,
        LEAD(days_since_end_date, 3) OVER w_product_holder_id_by_min_day AS next3_days_since_end_date,
        SUM(days_since_end_date) OVER (PARTITION BY holder_id ORDER BY last_known_days_since_end_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_end_date_sum_7,
        LAST_VALUE(end_date) OVER (PARTITION BY MONTH(end_date), status ORDER BY prev3_days_since_end_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        LEAD(prev_days_since_end_date, 3, 0) OVER (PARTITION BY holder_id ORDER BY last_known_days_since_end_date ASC, id) AS next3_prev_days_since_end_date,
        NTILE(4) OVER (PARTITION BY holder_id ORDER BY days_since_end_date ASC, id) AS days_since_end_date_quartile,
        ROUND(days_since_end_date_sum_4 - AVG(days_since_end_date_sum_4) OVER (PARTITION BY product), 2) AS days_since_end_date_sum_4_vs_product_avg
    FROM ranked
    WINDOW w_product_holder_id_by_min_day AS (PARTITION BY product, holder_id ORDER BY min_days_since_end_date_so_far, id)
)
-- Final projection
SELECT
    id,
    days_since_end_date_share_of_product_holder_id,
    last_known_days_since_end_date,
    days_since_prev_end_date,
    end_date_percent_rank,
    status_max_prev3_days_since_end_date,
    prev3_days_since_end_date_percent_rank,
    next3_days_since_end_date,
    days_since_end_date_sum_7,
    last_end_date,
    days_since_end_date_quartile,
    days_since_end_date_sum_4_vs_product_avg,
    CASE WHEN end_date_percent_rank >= 0.8 THEN 'large' WHEN end_date_percent_rank >= 0.3 THEN 'medium' ELSE 'small' END AS end_date_percent_rank_tier,
    ROUND(days_since_prev_end_date / NULLIF(end_date_percent_rank, 0), 3) AS days_since_prev_end_date_per_end_date_percent_ra
FROM with_windows
WHERE end_date IS NOT NULL
ORDER BY id, days_since_end_date_sum_4_vs_product_avg
LIMIT 5

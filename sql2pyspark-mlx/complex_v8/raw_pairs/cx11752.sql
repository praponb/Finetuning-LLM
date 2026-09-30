WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('HOME', 'TRAVEL') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE status IN ('ACTIVE', 'CANCELLED', 'LAPSED')
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, p.region AS holder_region
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        premium_filled,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id_product, 0), 4) AS premium_share_of_holder_id_product,
        NTILE(5) OVER (PARTITION BY holder_region ORDER BY start_date DESC, id) AS start_date_quintile,
        NTILE(5) OVER (PARTITION BY MONTH(end_date), holder_id ORDER BY premium_filled DESC, id) AS premium_filled_quintile,
        SIZE(COLLECT_SET(product_group) OVER (PARTITION BY holder_id, product)) AS distinct_product_group_in_holder_id_product,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_region)) AS distinct_product_in_holder_region,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY MONTH(end_date) ORDER BY premium_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LEAD(end_date, 3) OVER (PARTITION BY MONTH(end_date) ORDER BY premium_filled ASC, id) AS next3_end_date,
        ROUND(AVG(premium) OVER (PARTITION BY holder_region), 2) AS holder_region_avg_premium,
        ROUND(premium_filled / NULLIF(SUM(premium_filled) OVER (PARTITION BY MONTH(end_date)), 0), 4) AS premium_filled_share_of_end_date_month,
        premium_filled - LAG(premium_filled) OVER w_holder_region_by_start_date AS premium_filled_change,
        SUM(premium_filled) OVER w_end_date_month AS end_date_month_sum_premium_filled,
        premium_filled - LAG(premium_filled) OVER (PARTITION BY product ORDER BY premium, id) AS premium_filled_change_2,
        COUNT(*) OVER w_holder_id_product AS rows_in_holder_id_product,
        LEAD(end_date, 2) OVER w_end_date_month_holder_id_by AS next2_end_date,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(end_date), holder_id ORDER BY premium_filled DESC), 3) AS premium_filled_cume_dist,
        SUM(premium) OVER (PARTITION BY product_group, status ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_sum_6_prior
    FROM joined
    WINDOW w_holder_id_product AS (PARTITION BY holder_id, product), w_holder_region_by_start_date AS (PARTITION BY holder_region ORDER BY start_date DESC, id), w_end_date_month AS (PARTITION BY MONTH(end_date)), w_end_date_month_holder_id_by AS (PARTITION BY MONTH(end_date), holder_id ORDER BY premium_filled DESC, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id, distinct_product_in_holder_region)) AS distinct_product_in_holder_id_distinct_product_i,
        MIN(start_date_quintile) OVER w_holder_id_distinct_product_i AS holder_id_distinct_product_in_holder_region_min_,
        SUM(premium_filled_cume_dist) OVER w_holder_id_distinct_product_i AS holder_id_distinct_product_in_holder_region_sum_,
        SUM(holder_region_avg_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_end_date) ORDER BY premium_filled_cume_dist ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_region_avg_premium,
        SUM(premium_filled_quintile) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_end_date) ORDER BY premium_filled_cume_dist ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_filled_quintile,
        SUM(premium_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_end_date) ORDER BY premium_filled_cume_dist ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_filled,
        ROW_NUMBER() OVER (PARTITION BY distinct_product_in_holder_region, MONTH(start_date) ORDER BY start_date_quintile ASC, id) AS seq_no
    FROM windowed
    WINDOW w_holder_id_distinct_product_i AS (PARTITION BY holder_id, distinct_product_in_holder_region)
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    premium_filled,
    premium_share_of_holder_id_product,
    distinct_product_group_in_holder_id_product,
    distinct_product_in_holder_region,
    second_start_date,
    next3_end_date,
    holder_region_avg_premium,
    premium_filled_share_of_end_date_month,
    premium_filled_change,
    end_date_month_sum_premium_filled,
    premium_filled_change_2,
    rows_in_holder_id_product,
    next2_end_date,
    premium_filled_cume_dist,
    premium_sum_6_prior,
    distinct_product_in_holder_id_distinct_product_i,
    running_holder_region_avg_premium,
    running_premium_filled_quintile,
    running_premium_filled,
    seq_no,
    DATEDIFF(DATE '2026-01-01', second_start_date) AS days_since_second_start_date
FROM with_trends

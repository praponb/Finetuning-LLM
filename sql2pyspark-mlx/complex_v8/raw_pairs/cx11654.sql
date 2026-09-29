WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ADD_MONTHS(end_date, 12) AS end_date_plus_12m
    FROM policies
    WHERE product IN ('HEALTH', 'LIFE')
      AND start_date < DATE '2025-05-30'
),
enriched AS (
    -- Attach holder attributes
    SELECT s.*, p.region AS holder_region
    FROM valid s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_region, product), 0), 4) AS premium_share_of_holder_region_product,
        ROUND(AVG(premium) OVER (PARTITION BY holder_region, product), 2) AS holder_region_product_avg_premium,
        SIZE(COLLECT_SET(holder_id) OVER w_holder_region_product) AS distinct_holder_id_in_holder_region_product,
        SUM(premium) OVER (PARTITION BY holder_region, product) AS holder_region_product_sum_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_region, product), 2) AS premium_vs_holder_region_product_avg,
        ROW_NUMBER() OVER w_all_by_end_date AS end_date_seq
    FROM enriched
    WINDOW w_holder_region_product AS (PARTITION BY holder_region, product), w_all_by_end_date AS (ORDER BY end_date ASC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE end_date_seq <= 5
)
SELECT id, metric_name, reading
FROM (
    SELECT id, premium_share_of_holder_region_product, holder_region_product_avg_premium
    FROM top_per_group
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (premium_share_of_holder_region_product, holder_region_product_avg_premium)
) unp

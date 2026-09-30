WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        brand,
        cost,
        launch_date,
        tags,
        CONCAT_WS(',', category, brand) category_brand_key
    FROM products
    WHERE supplier_id IS NULL
      AND launch_date >= TO_DATE('2023-06-28')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cost_avg_3,
        SUM(cost) OVER (PARTITION BY brand, category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_cost,
        PERCENT_RANK() OVER (PARTITION BY brand ORDER BY cost NULLS FIRST) cost_percent_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY category), 4) cost_share
    FROM base
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        cost,
        launch_date,
        tags,
        category_brand_key,
        running_cost,
        cost_percent_rank,
        CEIL(cost_share) cost_share_whole,
        ROUND(cost_share, 0) cost_share_rounded
    FROM scored
    WHERE running_cost BETWEEN 2000 AND 2779
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost_percent_rank DESC, id) AS rn
    FROM filtered
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    COUNT(CASE WHEN category = 'Books' THEN 1 END) AS books_count,
    AVG(cost_share_whole) FILTER (WHERE brand = 'Umbrella') AS umbrella_avg_cost_share_whole,
    COUNT(DISTINCT brand) AS distinct_brands,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    COUNT(*) AS row_count,
    MAX(running_cost) AS max_running_cost
FROM latest
GROUP BY activity_month
ORDER BY books_count DESC NULLS LAST;

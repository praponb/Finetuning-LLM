WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        last_restock,
        DAY(last_restock) AS last_restock_day
    FROM inventory
    WHERE last_restock >= '2023-08-22'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM cleaned
    WHERE last_restock <= '2024-10-01'
),
valid_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock_day,
        QUARTER(last_restock) AS last_restock_quarter
    FROM valid
    WHERE last_restock > DATE '2023-06-12'
      AND last_restock_day >= 56
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(last_restock_quarter) OVER (PARTITION BY product_id ORDER BY last_restock_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last_restock_quarter,
        SUM(last_restock_quarter) OVER (PARTITION BY product_id ORDER BY last_restock_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last_restock_quarter,
        COUNT(last_restock_quarter) OVER (PARTITION BY product_id ORDER BY last_restock_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last_restock_quarter,
        AVG(last_restock_quarter) OVER (PARTITION BY product_id ORDER BY last_restock_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last_restock_quarter
    FROM valid_inventory
    WHERE product_id IN (3, 5)
),
enriched_inventory AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.last_restock_day,
        src.last_restock_quarter,
        src.running_min_last_restock_quarter,
        src.running_sum_last_restock_quarter,
        src.running_avg_last_restock_quarter,
        d.category
    FROM enriched src
    LEFT JOIN products d
        ON src.product_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(running_sum_last_restock_quarter) OVER w_ordered AS running_count_running_sum_last_restock_quarter,
        MAX(running_sum_last_restock_quarter) OVER w_ordered AS running_max_running_sum_last_restock_quarter,
        MIN(running_sum_last_restock_quarter) OVER w_ordered AS running_min_running_sum_last_restock_quarter,
        RANK() OVER w_ordered_2 AS running_avg_last_restock_quarter_rank
    FROM enriched_inventory
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY last_restock_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY product_id ORDER BY running_avg_last_restock_quarter DESC)
)
-- Join a dimension and aggregate
SELECT
    p.brand,
    e.category AS inventory_category,
    COUNT(*) AS cnt,
    MIN(e.running_min_last_restock_quarter) AS min_running_min_last_restock_quarter,
    MIN(e.running_sum_last_restock_quarter) AS min_running_sum_last_restock_quarter
FROM scored e
INNER JOIN products p ON e.product_id = p.id
GROUP BY p.brand, e.category

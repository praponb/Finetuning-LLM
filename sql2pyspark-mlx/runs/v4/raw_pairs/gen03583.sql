WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST, id) AS latest_rank
    FROM inventory
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, p.category
    FROM ranked s
    LEFT JOIN products p
        ON s.product_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id) quantity_bucket,
        LEAD(quantity, 1) OVER w_hist next_quantity
    FROM enriched
    WINDOW w_hist AS (ORDER BY last_restock, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(quantity) OVER w_ordered category_avg_quantity,
        LAST_VALUE(next_quantity, TRUE) OVER (PARTITION BY store_id, category ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_next_quantity,
        LEAD(next_quantity, 1) OVER w_ordered_2 next_next_quantity
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY category), w_ordered_2 AS (PARTITION BY category ORDER BY next_quantity, id)
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        store_id,
        DATE_TRUNC('YEAR', last_restock) AS year_start,
        DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
        APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50,
        COUNT(*) AS row_count,
        COUNT(*) AS num_rows,
        SIZE(COLLECT_SET(category)) AS num_categories,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING_ID() AS grouping_id
    FROM scored
    GROUP BY GROUPING SETS ((store_id, DATE_TRUNC('YEAR', last_restock), DATE_TRUNC('QUARTER', last_restock)), (store_id, DATE_TRUNC('YEAR', last_restock)), (store_id), ())
)
SELECT
    store_id,
    year_start,
    quarter_start,
    quantity_p50,
    row_count,
    num_rows,
    distinct_ids
FROM grouped
ORDER BY distinct_ids DESC

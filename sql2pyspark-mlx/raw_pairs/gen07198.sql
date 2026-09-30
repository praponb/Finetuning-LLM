WITH kept_keys AS (
    -- Set difference on keys
    SELECT store_id, product_id
    FROM inventory
    EXCEPT DISTINCT
    SELECT store_id, product_id
    FROM inventory
    WHERE last_restock BETWEEN '2023-08-10' AND '2025-10-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.cost,
        p.category,
        p.brand
    FROM reconciled src
    LEFT JOIN products p
        ON src.product_id = p.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        cost,
        category,
        QUARTER(last_restock) last_restock_quarter
    FROM joined
    WHERE last_restock >= '2023-08-08'
      AND category IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY last_restock_quarter DESC NULLS FIRST) last_restock_quarter_dense_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY DATE_TRUNC('month', last_restock), product_id), 4) cost_share
    FROM valid
),
with_windows_inventory AS (
    -- Running and moving aggregates
    SELECT
        quantity,
        cost,
        category,
        last_restock_quarter,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) quantity_rank,
        MAX(cost) OVER (PARTITION BY category) category_max_cost,
        PERCENT_RANK() OVER (ORDER BY cost_share DESC NULLS LAST) cost_share_percent_rank
    FROM with_windows
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    category,
    metric,
    val
FROM (
    SELECT
        category,
        cost,
        cost_share_percent_rank
    FROM with_windows_inventory
) src
UNPIVOT (
    val FOR metric IN (cost, cost_share_percent_rank)
)

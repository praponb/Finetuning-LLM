WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        LAST_DAY(last_restock) last_restock_month_end
    FROM inventory
    WHERE last_restock >= DATE '2024-02-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity * 0.85, 2) adjusted_quantity
    FROM valid
    WHERE store_id IN (2, 3, 4, 5)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS FIRST, id) = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.brand,
        d.price,
        d.category
    FROM deduped s
    LEFT JOIN products d
        ON s.product_id = d.id
),
deduped_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, category ORDER BY last_restock DESC NULLS LAST, id) AS dedup_rank
    FROM with_dims
    QUALIFY dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', last_restock_month_end) AS year_start,
    COUNT(*) AS event_count,
    COUNT(DISTINCT category) AS distinct_categories,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM deduped_inventory
GROUP BY DATE_TRUNC('YEAR', last_restock_month_end);

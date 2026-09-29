WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-07-18')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS LAST, id) AS row_num
    FROM base
    QUALIFY row_num = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        quantity,
        row_num
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC NULLS FIRST, id) = 1
),
latest_inventory AS (
    -- Keep one row per key (latest first)
    SELECT id, product_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS dedup_rank
        FROM deduped
    ) t
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    product_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS cnt
FROM latest_inventory
GROUP BY product_id
ORDER BY event_count DESC NULLS FIRST

WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity NULLS LAST, id) = 1
)
-- Grouped metrics
SELECT
    store_id,
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    BIT_OR(quantity) AS quantity_bit_or,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    GROUPING_ID() AS grouping_level
FROM ranked
GROUP BY GROUPING SETS ((store_id, DATE_TRUNC('WEEK', last_restock)), (store_id))

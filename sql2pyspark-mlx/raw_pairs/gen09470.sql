WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id, store_id
    FROM inventory
    INTERSECT ALL
    SELECT id, store_id
    FROM inventory
    WHERE store_id = 1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM kept_rows
GROUP BY store_id
ORDER BY store_id ASC NULLS FIRST

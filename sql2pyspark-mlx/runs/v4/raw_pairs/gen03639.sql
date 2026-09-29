WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM inventory
    INTERSECT
    SELECT product_id
    FROM inventory
    WHERE product_id = 9
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    store_id,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    BIT_OR(quantity) AS quantity_bit_or,
    GROUPING(product_id) AS gid
FROM verified
GROUP BY GROUPING SETS ((product_id, store_id, DATE_TRUNC('MONTH', last_restock)), (product_id, store_id), ())
ORDER BY activity_month NULLS LAST;

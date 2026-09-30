WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id
    FROM inventory
    INTERSECT ALL
    SELECT store_id
    FROM inventory
    WHERE last_restock BETWEEN '2024-01-01' AND '2024-10-02'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id
)
-- Multi-level aggregation
SELECT
    product_id,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95
FROM verified
GROUP BY 1
ORDER BY quantity_bit_and NULLS FIRST;

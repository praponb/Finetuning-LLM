WITH kept_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM inventory
    EXCEPT
    SELECT product_id
    FROM inventory
    WHERE product_id IN (4, 7, 9)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN kept_keys k ON s.product_id = k.product_id
)
-- Grouped metrics
SELECT
    store_id,
    product_id,
    BIT_AND(quantity) AS quantity_bit_and,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    COUNT(*) AS row_count,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM verified
GROUP BY store_id, product_id
ORDER BY distinct_product_ids NULLS FIRST;

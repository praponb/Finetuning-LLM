WITH remaining_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM store_sales
    EXCEPT DISTINCT
    SELECT product_id
    FROM store_sales
    WHERE store_id IN (1, 2, 5, 6)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue
FROM kept_rows;

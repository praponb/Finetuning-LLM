WITH base AS (
    SELECT
        id,
        quantity,
        CEIL(quantity) quantity_whole,
        CAST(quantity AS DOUBLE) quantity_str
    FROM inventory
    WHERE quantity > 234
),
base_inventory AS (
    SELECT *, CEIL(quantity_str) quantity_str_whole
    FROM base
    WHERE quantity_whole > 21
),
valid AS (
    SELECT
        *,
        quantity_str + quantity quantity_str_plus_quantity,
        quantity_str_whole + quantity quantity_str_whole_plus_quantity
    FROM base_inventory
    WHERE quantity BETWEEN 159 AND 199
),
kept_keys AS (
    SELECT id
    FROM valid
    EXCEPT DISTINCT
    SELECT id
    FROM inventory
    WHERE product_id IN (6, 7, 8)
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id
),
summary AS (
    SELECT
        CASE WHEN quantity_str_whole_plus_quantity >= 69 THEN 'LARGE' ELSE 'SMALL' END AS quantity_str_whole_plus_quantity_tier,
        CASE WHEN quantity_str >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_str_tier,
        MIN(quantity) AS min_quantity,
        BIT_AND(quantity) AS quantity_bit_and,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM kept_rows
    GROUP BY CASE WHEN quantity_str_whole_plus_quantity >= 69 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN quantity_str >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    quantity_str_whole_plus_quantity_tier,
    quantity_str_tier,
    min_quantity,
    distinct_ids_2
FROM summary
ORDER BY min_quantity DESC, quantity_str_tier;

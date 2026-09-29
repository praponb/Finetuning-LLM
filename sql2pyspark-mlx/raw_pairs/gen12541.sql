WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE order_id IN (26, 27, 30, 35)
),
filtered_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        greatest_quantity_unit_price,
        unit_price / NULLIF(greatest_quantity_unit_price, 0) AS unit_price_per_greatest_quantity_unit_price
    FROM filtered
    WHERE quantity <= 3
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        greatest_quantity_unit_price,
        unit_price_per_greatest_quantity_unit_price,
        LEAD(quantity) OVER w_part AS next_quantity,
        LAG(quantity) OVER w_part AS prev_quantity,
        LEAD(unit_price_per_greatest_quantity_unit_price) OVER w_part AS next_unit_price_per_greatest_quantity_unit_price,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM filtered_order_items
    WINDOW w_part AS (PARTITION BY product_id ORDER BY greatest_quantity_unit_price ASC, id ASC)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        greatest_quantity_unit_price,
        next_quantity,
        prev_quantity,
        next_unit_price_per_greatest_quantity_unit_price,
        CAST(next_quantity AS DOUBLE) AS next_quantity_str
    FROM with_windows
    WHERE next_unit_price_per_greatest_quantity_unit_price > 76
)
-- Window metrics over several different partitions
SELECT
    *,
    FIRST_VALUE(next_unit_price_per_greatest_quantity_unit_price) IGNORE NULLS OVER (ORDER BY greatest_quantity_unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_unit_price_per_greatest_quantity_unit_price,
    NTILE(3) OVER (ORDER BY next_quantity_str DESC, id ASC) AS next_quantity_str_bucket
FROM valid
WHERE next_quantity_str BETWEEN 47 AND 61;

WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price < 370
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        unit_price,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        LEAD(unit_price, 1) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS next_unit_price,
        RANK() OVER w_recent AS line_total_rank,
        LAST_VALUE(unit_price, TRUE) OVER w_grp AS last_known_unit_price
    FROM valid
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY line_total), w_grp AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        unit_price,
        next_unit_price,
        line_total_rank,
        last_known_unit_price,
        ROUND(last_known_quantity, 1) AS last_known_quantity_rounded
    FROM metrics
    WHERE unit_price BETWEEN 110 AND 160
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(last_known_unit_price) AS last_known_unit_price_whole,
        CAST(next_unit_price AS INT) AS next_unit_price_int
    FROM cleaned
    WHERE quantity BETWEEN 5 AND 6
),
base_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        unit_price,
        next_unit_price,
        last_known_unit_price,
        last_known_quantity_rounded,
        last_known_unit_price_whole,
        next_unit_price_int,
        ROUND(quantity / unit_price, 2) AS line_total
    FROM base
    WHERE line_total_rank IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN line_total >= 81 THEN 'LARGE' ELSE 'SMALL' END AS line_total_tier,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(*) AS num_rows,
    MAX(last_known_unit_price_whole) AS max_last_known_unit_price_whole
FROM base_order_items
GROUP BY 1
ORDER BY quantity_bit_and DESC;

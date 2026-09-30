WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        unit_price,
        FLOOR(unit_price) AS unit_price_whole,
        unit_price * quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_price,
        unit_price_whole,
        line_total,
        LAST_VALUE(line_total) IGNORE NULLS OVER w_hist AS last_known_line_total,
        COUNT(line_total) OVER w_part AS order_id_count_line_total,
        MIN(unit_price) OVER w_part AS order_id_min_unit_price
    FROM valid
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY order_id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY unit_price_whole DESC, id) AS unit_price_whole_bucket,
        LEAD(order_id_min_unit_price, 1) OVER w_hist AS next_order_id_min_unit_price,
        LEAD(order_id_min_unit_price, 2) OVER (ORDER BY last_known_line_total, id) AS next_order_id_min_unit_price_2,
        DENSE_RANK() OVER (ORDER BY order_id_min_unit_price NULLS FIRST) AS order_id_min_unit_price_dense_rank
    FROM with_windows
    WINDOW w_hist AS (ORDER BY order_id_count_line_total, id)
),
valid_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        unit_price,
        unit_price_whole,
        line_total,
        last_known_line_total,
        order_id_min_unit_price,
        unit_price_whole_bucket,
        next_order_id_min_unit_price,
        next_order_id_min_unit_price_2,
        order_id_min_unit_price_dense_rank,
        order_id_count_line_total - order_id_min_unit_price AS order_id_count_line_total_minus_order_id_min_unit_price
    FROM scored
    WHERE last_known_line_total < 54
),
valid_order_items_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        unit_price,
        line_total,
        unit_price_whole_bucket,
        next_order_id_min_unit_price,
        next_order_id_min_unit_price_2,
        order_id_min_unit_price_dense_rank,
        order_id_count_line_total_minus_order_id_min_unit_price,
        GREATEST(order_id_min_unit_price, unit_price_whole) AS greatest_order_id_min_unit_price_unit_price_whole
    FROM valid_order_items
    WHERE unit_price_whole_bucket <= 6
)
SELECT
    id,
    line_total,
    unit_price_whole_bucket,
    next_order_id_min_unit_price,
    next_order_id_min_unit_price_2,
    order_id_count_line_total_minus_order_id_min_unit_price,
    greatest_order_id_min_unit_price_unit_price_whole
FROM valid_order_items_2
ORDER BY id NULLS FIRST;

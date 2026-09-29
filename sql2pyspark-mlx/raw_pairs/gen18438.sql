WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        unit_price + quantity line_total,
        quantity * unit_price line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        line_total_2,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) quantity_avg_5_prior,
        FIRST_VALUE(unit_price, TRUE) OVER (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_unit_price,
        LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY line_total_2, id) prev_quantity
    FROM valid
),
valid_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(quantity_avg_5_prior, 0) quantity_avg_5_prior_rounded
    FROM with_windows
    WHERE prev_quantity <= 7
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        line_total_2,
        first_known_unit_price,
        prev_quantity,
        quantity_avg_5_prior_rounded,
        AVG(line_total_2) OVER w_grp running_avg_line_total_2,
        MIN(line_total_2) OVER w_grp running_min_line_total_2,
        COUNT(line_total_2) OVER w_grp running_count_line_total_2
    FROM valid_order_items
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY quantity_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    first_known_unit_price,
    running_avg_line_total_2,
    running_min_line_total_2
FROM metrics;

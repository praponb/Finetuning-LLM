WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        MAX(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity,
        MIN(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        LAG(unit_price, 1, 0) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) AS prev_unit_price
    FROM order_items
),
scored_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(running_min_quantity, TRUE) OVER (ORDER BY running_min_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_min_quantity,
        FIRST_VALUE(running_min_quantity) IGNORE NULLS OVER (ORDER BY running_max_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_min_quantity_2,
        AVG(prev_unit_price) OVER (ORDER BY prev_unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev_unit_price_avg_6_prior
    FROM scored
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(prev_unit_price / running_sum_quantity, 2) AS prev_unit_price_per_running_sum_quantity
    FROM scored_order_items
    WHERE prev_unit_price_avg_6_prior BETWEEN 123 AND 284
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT ALL
    SELECT id
    FROM order_items
    WHERE quantity IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    quantity,
    running_min_quantity,
    prev_unit_price,
    first_known_running_min_quantity_2,
    prev_unit_price_avg_6_prior,
    prev_unit_price_per_running_sum_quantity
FROM reconciled
ORDER BY running_min_quantity DESC NULLS FIRST, prev_unit_price_avg_6_prior;

WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        ROUND(unit_price * 1.1, 2) AS unit_price_adjusted
    FROM order_items
    WHERE unit_price <= 190
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        MAX(quantity) OVER (PARTITION BY product_id, order_id) AS product_id_max_quantity
    FROM filtered
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_quantity, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
scored_order_items AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id_max_quantity,
        rank_in_group,
        LEAD(quantity, 1) OVER w_hist AS next_quantity,
        LAST_VALUE(product_id_max_quantity, TRUE) OVER (PARTITION BY product_id ORDER BY product_id_max_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_id_max_quantity,
        LEAD(running_quantity, 2) OVER w_hist AS next_running_quantity
    FROM ranked
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY quantity, id)
)
-- Ranking and gap features
SELECT
    *,
    AVG(product_id_max_quantity) OVER (ORDER BY product_id_max_quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS product_id_max_quantity_avg_3,
    NTILE(3) OVER (ORDER BY next_quantity DESC, id) AS next_quantity_bucket
FROM scored_order_items
WHERE product_id_max_quantity <= 6

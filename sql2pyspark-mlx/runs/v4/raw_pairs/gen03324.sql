WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(unit_price) OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_avg_2_prior,
        FIRST_VALUE(unit_price, TRUE) OVER w_hist AS first_known_unit_price,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id), 4) AS quantity_share
    FROM order_items
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.category,
        p.stock_qty,
        p.cost
    FROM scored e
    INNER JOIN products p
        ON e.product_id = p.id
),
scored_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(unit_price, 1) OVER (PARTITION BY order_id ORDER BY first_known_unit_price, id) AS prev_unit_price,
        LAST_VALUE(category, TRUE) OVER (PARTITION BY product_id ORDER BY unit_price_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_category,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY category, product_id), 4) AS quantity_share_2
    FROM enriched
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY stock_qty DESC) AS rank_in_group
        FROM scored_order_items
    ) t
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.unit_price_avg_2_prior,
    s.first_known_unit_price,
    s.quantity_avg_3_prior,
    s.stock_qty,
    s.prev_unit_price,
    (SELECT MIN(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS min_inventory_quantity
FROM ranked s;

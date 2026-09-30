WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(unit_price, 2) AS unit_price_rounded
    FROM order_items
    WHERE product_id = 28
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST, id) AS rn
        FROM filtered
    ) x
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        unit_price_rounded,
        rn,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity,
        MIN(quantity) OVER (PARTITION BY order_id ORDER BY unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        COUNT(quantity) OVER (PARTITION BY order_id ORDER BY unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity
    FROM deduped
    WHERE unit_price_rounded <= 41
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, order_id
    FROM scored
    INTERSECT ALL
    SELECT id, order_id
    FROM order_items
    WHERE order_id IN (7, 25, 37)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
)
-- Window metrics over several different partitions
SELECT
    order_id,
    unit_price_rounded,
    running_min_quantity,
    running_count_quantity,
    MAX(unit_price_rounded) OVER w_hist AS order_id_max_unit_price_rounded,
    CUME_DIST() OVER w_recent AS running_min_quantity_cume_dist
FROM verified
WINDOW w_hist AS (PARTITION BY order_id, product_id), w_recent AS (PARTITION BY order_id ORDER BY running_min_quantity NULLS LAST)

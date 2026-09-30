WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id) AS dedup_rank
        FROM order_items
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        MIN(unit_price) OVER w_hist AS last4_min_unit_price,
        AVG(unit_price) OVER w_hist AS last4_avg_unit_price,
        MAX(unit_price) OVER w_hist AS last4_max_unit_price,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price
    FROM deduped
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT ALL
    SELECT id
    FROM order_items
    WHERE product_id = 4
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM kept_rows
    INTERSECT
    SELECT id, product_id
    FROM order_items
    WHERE product_id IN (7, 12)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys_2 k ON s.id = k.id AND s.product_id = k.product_id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.product_id,
        e.quantity,
        e.unit_price,
        e.last4_min_unit_price,
        e.last4_max_unit_price,
        e.corr_quantity_unit_price,
        b.category,
        b.stock_qty,
        b.price
    FROM verified e
    LEFT JOIN products b
        ON e.product_id = b.id
)
SELECT
    product_id,
    quantity,
    unit_price,
    last4_max_unit_price,
    corr_quantity_unit_price,
    category,
    price
FROM with_dims

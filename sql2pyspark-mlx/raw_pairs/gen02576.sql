WITH enriched AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.quantity,
        b.stock_qty
    FROM inventory src
    JOIN products b
        ON src.product_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity) OVER w_recent AS prev_quantity,
        LEAD(quantity) OVER w_recent AS next_quantity,
        LAG(stock_qty) OVER w_recent AS prev_stock_qty,
        LEAD(stock_qty) OVER w_recent AS next_stock_qty,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM enriched
    WHERE product_id IN (3, 5, 10)
    WINDOW w_recent AS (PARTITION BY store_id, product_id ORDER BY quantity, id)
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_stock_qty, id) AS row_seq_2,
        LEAD(prev_stock_qty, 1) OVER (PARTITION BY product_id ORDER BY prev_quantity, id) AS next_prev_stock_qty,
        SUM(prev_quantity) OVER (PARTITION BY store_id, product_id ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_quantity
    FROM scored
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.quantity,
        e.prev_quantity,
        e.next_quantity,
        e.next_stock_qty,
        e.next_prev_stock_qty,
        e.running_prev_quantity,
        p.sqft
    FROM enriched_inventory e
    JOIN stores p
        ON e.store_id = p.id
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    next_quantity,
    next_stock_qty,
    next_prev_stock_qty,
    sqft
FROM with_dims

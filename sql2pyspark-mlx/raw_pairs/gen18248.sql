WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(unit_price / quantity, 2) AS line_total,
        quantity - unit_price AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.line_total,
        p.price,
        p.stock_qty
    FROM cleaned a
    INNER JOIN products p
        ON a.product_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price NULLS FIRST, id) AS rank_in_group
        FROM with_dims
    ) ranked
    WHERE rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        line_total,
        price,
        LEAD(stock_qty, 1) OVER (PARTITION BY product_id ORDER BY stock_qty, id) AS next_stock_qty,
        NTILE(5) OVER (PARTITION BY product_id, order_id ORDER BY stock_qty, id) AS stock_qty_bucket,
        CUME_DIST() OVER (ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_cume_dist,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty
    FROM deduped
    WHERE product_id IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        line_total,
        price,
        next_stock_qty,
        stock_qty_bucket,
        stock_qty_cume_dist,
        last_known_stock_qty,
        next_stock_qty + last_known_stock_qty AS next_stock_qty_plus_last_known_stock_qty,
        last_known_stock_qty + next_stock_qty AS last_known_stock_qty_plus_next_stock_qty
    FROM metrics
    WHERE last_known_stock_qty < 103
),
metrics_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(next_stock_qty_plus_last_known_stock_qty) OVER (ORDER BY last_known_stock_qty_plus_next_stock_qty, id) AS prev_next_stock_qty_plus_last_known_stock_qty,
        LEAD(next_stock_qty_plus_last_known_stock_qty) OVER (ORDER BY last_known_stock_qty_plus_next_stock_qty, id) AS next_next_stock_qty_plus_last_known_stock_qty,
        LAG(stock_qty_cume_dist) OVER (ORDER BY last_known_stock_qty_plus_next_stock_qty, id) AS prev_stock_qty_cume_dist,
        ROW_NUMBER() OVER (ORDER BY last_known_stock_qty_plus_next_stock_qty, id) AS seq_no
    FROM filtered
)
SELECT
    id,
    line_total,
    price,
    stock_qty_cume_dist,
    last_known_stock_qty,
    next_stock_qty_plus_last_known_stock_qty,
    last_known_stock_qty_plus_next_stock_qty,
    prev_next_stock_qty_plus_last_known_stock_qty,
    prev_stock_qty_cume_dist
FROM metrics_order_items
ORDER BY next_stock_qty_plus_last_known_stock_qty NULLS LAST, last_known_stock_qty_plus_next_stock_qty DESC

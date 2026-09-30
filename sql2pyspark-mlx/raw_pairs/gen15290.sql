WITH with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity,
        d.category,
        d.stock_qty
    FROM order_items e
    LEFT JOIN products d
        ON e.product_id = d.id
    WHERE ARRAY_CONTAINS(d.tags, 'new')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        category,
        stock_qty,
        COALESCE(category, 'Unknown') category_filled,
        COALESCE(category, 'N/A') category_clean
    FROM with_dims
    WHERE stock_qty < 101
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        MAX(stock_qty) OVER w_part running_max_stock_qty,
        COUNT(stock_qty) OVER w_part running_count_stock_qty,
        MIN(stock_qty) OVER w_part running_min_stock_qty,
        SUM(stock_qty) OVER w_part running_sum_stock_qty,
        SUM(stock_qty) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_stock_qty
    FROM cleaned
    WINDOW w_part AS (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims_order_items AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.running_count_stock_qty,
        src.running_stock_qty,
        d.brand,
        d.price,
        d.stock_qty
    FROM enriched src
    LEFT JOIN products d
        ON src.product_id = d.id
)
SELECT
    id,
    product_id,
    running_count_stock_qty,
    running_stock_qty,
    brand,
    price
FROM with_dims_order_items
ORDER BY price NULLS LAST

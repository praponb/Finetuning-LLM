WITH with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand,
        s.price,
        s.stock_qty,
        s.tags,
        x.order_item_quantity,
        x.unit_price
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(order_item_quantity) OVER (PARTITION BY brand ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_order_item_quantity,
        CORR(stock_qty, price) OVER w_grp AS corr_stock_qty_price,
        SUM(unit_price) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price
    FROM with_last
    WINDOW w_grp AS (PARTITION BY brand)
),
with_last_products AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.units,
        latest.store_sale_sale_date
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        brand,
        price,
        stock_qty,
        corr_stock_qty_price,
        running_unit_price,
        units,
        store_sale_sale_date
    FROM with_last_products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC NULLS FIRST, id) = 1
),
ranked_products AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked_products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.product_id IN (3, 5, 9, 10)
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        price,
        stock_qty,
        corr_stock_qty_price,
        running_unit_price,
        units,
        dedup_rank,
        SUM(units) OVER (PARTITION BY brand ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
        NTILE(3) OVER (PARTITION BY brand ORDER BY corr_stock_qty_price DESC, id) AS corr_stock_qty_price_bucket,
        ROUND(units / SUM(units) OVER (PARTITION BY brand), 4) AS units_share,
        LAG(price, 1, 0) OVER (ORDER BY price, id) AS prev_price,
        PERCENT_RANK() OVER (PARTITION BY brand ORDER BY price NULLS FIRST) AS price_percent_rank
    FROM kept
)
SELECT
    id,
    price,
    corr_stock_qty_price,
    running_unit_price,
    units,
    corr_stock_qty_price_bucket,
    units_share,
    prev_price,
    price_percent_rank
FROM metrics
ORDER BY price_percent_rank DESC NULLS FIRST, corr_stock_qty_price_bucket

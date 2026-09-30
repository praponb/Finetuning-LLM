WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(unit_price, 0) AS unit_price_filled,
        ROUND(unit_price, 2) AS unit_price_rounded
    FROM order_items
    WHERE product_id IN (13, 18, 27)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.order_id,
        e.product_id,
        e.unit_price,
        e.unit_price_rounded,
        b.cost,
        b.stock_qty
    FROM cleaned e
    INNER JOIN products b
        ON e.product_id = b.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY stock_qty DESC NULLS FIRST, id) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        unit_price,
        stock_qty
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY stock_qty DESC, id) AS latest_rank
        FROM ranked
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.unit_price,
        s.stock_qty,
        p.brand
    FROM deduped s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.cost > 460
)
-- Join a dimension and aggregate
SELECT
    p.category,
    e.brand AS order_item_brand,
    COUNT(*) AS rows_total,
    SUM(e.unit_price) AS total_unit_price,
    ROUND(SUM(e.stock_qty), 2) AS total_stock_qty
FROM enriched e
LEFT JOIN products p ON e.product_id = p.id
GROUP BY GROUPING SETS ((p.category, e.brand), (p.category));

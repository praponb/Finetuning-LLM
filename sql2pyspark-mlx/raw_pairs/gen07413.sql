WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label
    FROM inventory
    WHERE last_restock >= DATE '2023-08-24'
      AND quantity <= 174
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.brand,
        b.stock_qty,
        b.launch_date
    FROM base a
    LEFT JOIN products b
        ON a.product_id = b.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.price,
        d.stock_qty AS product_stock_qty,
        d.product_name
    FROM enriched a
    LEFT JOIN products d
        ON a.product_id = d.id
    WHERE YEAR(d.launch_date) >= 2025
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand ORDER BY quantity DESC) AS latest_rank
        FROM with_dims
    ) x
    WHERE latest_rank = 1
),
with_dims_inventory AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.brand,
        e.stock_qty,
        e.price,
        e.product_stock_qty,
        e.latest_rank,
        p.category
    FROM latest e
    LEFT JOIN products p
        ON e.product_id = p.id
)
SELECT
    id,
    price,
    product_stock_qty
FROM with_dims_inventory

WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        MONTH(sale_date) AS sale_date_month,
        CEIL(revenue) AS revenue_whole
    FROM store_sales
    WHERE sale_date >= DATE '2023-09-11'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date_month DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.units,
        p.launch_date,
        p.stock_qty
    FROM deduped s
    LEFT JOIN products p
        ON s.product_id = p.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.launch_date,
        s.stock_qty,
        p.brand,
        p.stock_qty AS product_stock_qty,
        p.cost
    FROM enriched s
    JOIN products p
        ON s.product_id = p.id
    WHERE p.supplier_id IN (32, 37)
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(product_stock_qty) OVER (PARTITION BY store_id ORDER BY launch_date, id) AS next_product_stock_qty,
        LAG(product_stock_qty) OVER (PARTITION BY store_id ORDER BY launch_date, id) AS prev_product_stock_qty,
        LAG(stock_qty) OVER (PARTITION BY store_id ORDER BY launch_date, id) AS prev_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY launch_date, id) AS event_seq
    FROM with_dims
)
SELECT
    s.id,
    s.product_id,
    s.stock_qty,
    s.brand,
    s.cost,
    s.prev_product_stock_qty,
    s.prev_stock_qty,
    s.event_seq,
    CASE WHEN brand RLIKE '^(Globex|Stark)$' THEN 'PRIORITY' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS brand_zone,
    (SELECT MAX(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS max_inventory_quantity
FROM enriched_store_sales s

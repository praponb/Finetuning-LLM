WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM store_sales
    QUALIFY RANK() OVER (PARTITION BY store_id, product_id ORDER BY sale_date) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date NULLS LAST, id) = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT s.*, b.region
    FROM latest s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
latest_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC NULLS FIRST) = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.stock_qty,
        b.brand
    FROM latest_store_sales src
    LEFT JOIN products b
        ON src.product_id = b.id
),
ranked_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY units NULLS FIRST, id) = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.units,
    s.revenue,
    s.region,
    s.brand,
    (SELECT MAX(x.shipping_fee) FROM orders x WHERE x.store_id = s.store_id) AS max_order_shipping_fee
FROM ranked_store_sales s
ORDER BY region DESC NULLS FIRST;

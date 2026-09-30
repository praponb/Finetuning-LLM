WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('week', last_restock) AS last_restock_period
    FROM inventory
    WHERE store_id = 4
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        b.region,
        b.sqft
    FROM base s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_grp AS quantity_dense_rank,
        LAST_VALUE(region, TRUE) OVER w_ordered AS last_known_region,
        SUM(quantity) OVER (PARTITION BY TO_DATE(last_restock_period), region ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM joined
    WHERE last_restock_period < '2024-07-22'
    WINDOW w_grp AS (PARTITION BY region, store_id ORDER BY quantity NULLS LAST), w_ordered AS (PARTITION BY product_id, store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.sqft,
        s.last_known_region,
        s.cumulative_quantity,
        p.product_name,
        p.stock_qty
    FROM metrics s
    LEFT JOIN products p
        ON s.product_id = p.id
)
-- Ranking and gap features
SELECT
    id,
    product_id,
    quantity,
    last_known_region,
    product_name,
    stock_qty,
    FIRST_VALUE(stock_qty, TRUE) OVER (PARTITION BY product_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_stock_qty,
    STDDEV_SAMP(stock_qty) OVER (PARTITION BY product_id) AS product_id_stddev_samp_stock_qty,
    LAG(sqft, 1, 0) OVER (PARTITION BY product_id ORDER BY quantity, id) AS prev_sqft,
    NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket
FROM enriched

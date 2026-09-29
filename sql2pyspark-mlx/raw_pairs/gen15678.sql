WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        COALESCE(stock_qty, 0) AS stock_qty_filled,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM base
    EXCEPT ALL
    SELECT id, supplier_id
    FROM products
    WHERE category = 'Electronics'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
base_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        stock_qty_filled,
        brand_clean,
        SIZE(tags) AS tags_count,
        stock_qty_filled + cost AS stock_qty_filled_plus_cost
    FROM verified
    WHERE launch_date >= TO_DATE('2023-06-16')
      AND supplier_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty_filled,
        tags_count,
        COUNT(stock_qty_filled_plus_cost) OVER (PARTITION BY category) AS category_count_stock_qty_filled_plus_cost,
        PERCENT_RANK() OVER (ORDER BY cost DESC NULLS LAST) AS cost_percent_rank
    FROM base_products
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity = 6
        )
)
-- Running and moving aggregates
SELECT
    product_name,
    stock_qty_filled,
    cost_percent_rank,
    MAX(price) OVER (PARTITION BY brand) AS brand_max_price,
    DENSE_RANK() OVER (PARTITION BY brand ORDER BY category_count_stock_qty_filled_plus_cost DESC) AS category_count_stock_qty_filled_plus_cost_dense_rank,
    DENSE_RANK() OVER w_part AS stock_qty_filled_dense_rank
FROM eligible
WHERE category_count_stock_qty_filled_plus_cost BETWEEN 8 AND 10
WINDOW w_part AS (PARTITION BY brand ORDER BY stock_qty_filled DESC);

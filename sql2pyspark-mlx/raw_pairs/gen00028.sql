WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CEIL(quantity) AS quantity_whole,
        FLOOR(quantity) AS quantity_whole_2
    FROM inventory
    WHERE quantity >= 234
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity,
        e.quantity_whole,
        d.category,
        d.cost,
        d.stock_qty
    FROM valid e
    LEFT JOIN products d
        ON e.product_id = d.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        quantity_whole,
        stock_qty,
        quantity * cost AS quantity_times_cost
    FROM with_dims
    WHERE category IS NOT NULL
      AND quantity_whole > 62
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, b.brand
    FROM base a
    LEFT JOIN products b
        ON a.product_id = b.id
),
base_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(stock_qty, 0) AS stock_qty_filled,
        COALESCE(quantity_times_cost, 0) AS quantity_times_cost_filled,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM enriched
    WHERE brand IS DISTINCT FROM 'Globex'
      AND brand = 'Acme'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        quantity_whole,
        stock_qty,
        brand,
        brand_filled,
        LEAD(quantity_times_cost_filled, 1) OVER (PARTITION BY product_id ORDER BY quantity_times_cost, id) AS next_quantity_times_cost_filled,
        SUM(quantity) OVER (PARTITION BY product_id, brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM base_inventory
)
SELECT
    id,
    quantity_whole,
    stock_qty,
    brand,
    brand_filled,
    cumulative_quantity,
    CASE WHEN brand RLIKE '^(Acme|Globex)$' THEN 'PRIORITY' ELSE 'CORE' END AS brand_zone
FROM metrics
ORDER BY quantity_whole DESC NULLS FIRST, stock_qty;

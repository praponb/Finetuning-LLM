WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        tags,
        SUM(stock_qty) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        CUME_DIST() OVER (PARTITION BY supplier_id, category ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_cume_dist,
        SUM(cost) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM latest
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(category, ', ', product_name) AS category_product_name,
        ARRAY_CONTAINS(tags, 'bestseller') AS is_bestseller
    FROM scored
    WHERE product_name IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, cumulative_stock_qty + cost AS cumulative_stock_qty_plus_cost
    FROM base
    WHERE running_cost >= 2984
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (3, 4, 14, 25)
        )
),
latest_products AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cumulative_stock_qty DESC, id) AS latest_rank
        FROM eligible
    ) ranked
    WHERE latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty_cume_dist DESC, id) AS row_seq,
        AVG(price) OVER (PARTITION BY category ORDER BY running_cost, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS price_avg_5_prior,
        COUNT(cumulative_stock_qty_plus_cost) OVER (PARTITION BY category) AS category_count_cumulative_stock_qty_plus_cost
    FROM latest_products
    WHERE cumulative_stock_qty_plus_cost > 58
)
SELECT
    id,
    product_name,
    category,
    price,
    cost,
    cumulative_stock_qty,
    stock_qty_cume_dist,
    running_cost,
    category_product_name
FROM with_windows
ORDER BY cumulative_stock_qty NULLS FIRST

WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(stock_qty) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        CORR(cost, price) OVER w_recent AS corr_cost_price,
        ROUND(price / SUM(price) OVER (PARTITION BY supplier_id, brand), 4) AS price_share
    FROM products
    WHERE brand IS NULL
    WINDOW w_recent AS (PARTITION BY category)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        brand,
        stock_qty,
        launch_date,
        tags,
        cumulative_stock_qty,
        price_share,
        SUM(cumulative_stock_qty) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative_stock_qty,
        AVG(cost) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior
    FROM with_windows
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        brand,
        launch_date,
        cumulative_stock_qty,
        price_share,
        cost_avg_2_prior,
        DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM metrics
    WHERE launch_date < '2025-01-01'
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.launch_date,
        s.price_share,
        s.cost_avg_2_prior,
        s.days_since_launch_date,
        top1.order_item_unit_price,
        top1.order_item_quantity
    FROM base s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_products AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.days_since_launch_date,
        x.unit_price,
        x.quantity
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    days_since_launch_date,
    unit_price
FROM latest_match_products
ORDER BY unit_price NULLS FIRST, days_since_launch_date DESC

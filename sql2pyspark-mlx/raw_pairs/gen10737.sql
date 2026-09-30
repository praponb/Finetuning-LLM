WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        INITCAP(TRIM(product_name)) product_name_title
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS NOT DISTINCT FROM 'Umbrella'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        price,
        cost,
        supplier_id,
        DATE_ADD(launch_date, 46) launch_date_plus_46d,
        price / NULLIF(stock_qty, 0) price_per_stock_qty
    FROM filtered
    WHERE launch_date > DATE '2023-09-23'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date > '2024-09-01'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY category), 4) price_share,
        MAX(price) OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date_plus_46d), supplier_id) launch_date_plus_46d_period_max_price,
        AVG(price_per_stock_qty) OVER w_running category_avg_price_per_stock_qty
    FROM not_flagged
    WINDOW w_running AS (PARTITION BY category)
)
SELECT
    id,
    category,
    price,
    cost,
    supplier_id,
    launch_date_plus_46d,
    price_per_stock_qty,
    price_share,
    category_avg_price_per_stock_qty
FROM metrics;

WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(price, 0) AS price_filled,
        COALESCE(brand, 'DIRECT') AS brand_clean
    FROM products
    WHERE launch_date >= DATE '2023-07-21'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY brand ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY brand ORDER BY price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty_2,
        CORR(cost, price) OVER (PARTITION BY category) AS corr_cost_price,
        ROW_NUMBER() OVER (ORDER BY stock_qty DESC, id) AS seq_no
    FROM filtered
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date < s.launch_date
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        price_filled,
        brand_clean,
        cost_cume_dist,
        last_known_stock_qty_2,
        seq_no,
        ADD_MONTHS(launch_date, 3) AS launch_date_plus_months
    FROM eligible
    WHERE launch_date >= TO_DATE('2023-07-06')
      AND brand IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        category,
        price_filled,
        cost,
        price
    FROM base
) src
UNPIVOT (
    metric_value FOR kpi IN (price_filled AS PRICE_FILLED_VAL, cost AS COST_VAL, price AS PRICE_VAL)
)

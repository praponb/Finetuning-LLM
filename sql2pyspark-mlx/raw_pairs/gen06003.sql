WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, price - stock_qty AS price_minus_stock_qty
    FROM products
    WHERE launch_date >= DATE '2023-05-28'
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.sale_date,
        top1.units
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(price_minus_stock_qty) OVER w_running AS last4_max_price_minus_stock_qty,
        COUNT(price_minus_stock_qty) OVER w_running AS last4_count_price_minus_stock_qty,
        SUM(price_minus_stock_qty) OVER w_running AS last4_sum_price_minus_stock_qty
    FROM latest_match
    WINDOW w_running AS (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cost) OVER (PARTITION BY supplier_id ORDER BY cost, id) AS prev_cost,
        LEAD(last4_max_price_minus_stock_qty) OVER (PARTITION BY supplier_id ORDER BY cost, id) AS next_last4_max_price_minus_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost, id) AS event_seq
    FROM scored
    WHERE brand = 'Umbrella'
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.product_name,
        s.price,
        s.stock_qty,
        s.tags,
        s.supplier_id,
        s.sale_date,
        s.last4_max_price_minus_stock_qty,
        s.last4_sum_price_minus_stock_qty,
        s.prev_cost,
        p.country,
        p.supplier_name,
        p.rating
    FROM metrics s
    INNER JOIN suppliers p
        ON s.supplier_id = p.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    country,
    metric,
    metric_value
FROM (
    SELECT
        id,
        country,
        rating,
        last4_max_price_minus_stock_qty
    FROM with_dims
) src
UNPIVOT (
    metric_value FOR metric IN (rating AS RATING_VAL, last4_max_price_minus_stock_qty AS LAST4_MAX_PRICE_MINUS_STOCK_QTY)
);

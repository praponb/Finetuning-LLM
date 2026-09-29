WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(unit_price / quantity, 2) AS line_total,
        quantity * unit_price AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity = 6
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.cost,
        b.price,
        b.launch_date
    FROM filtered src
    LEFT JOIN products b
        ON src.product_id = b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY product_id), 4) AS price_share,
        LAG(price, 1, 0) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS prev_price,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM joined
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.amount,
        o.discount,
        o.order_date
    FROM metrics a
    LEFT JOIN orders o
        ON a.order_id = o.id
    WHERE o.channel != 'store'
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.quantity,
        a.unit_price,
        a.line_total,
        a.line_total_2,
        a.cost,
        a.price,
        a.launch_date,
        a.cumulative_quantity,
        a.amount,
        a.order_date,
        b.brand,
        b.stock_qty
    FROM with_dims a
    INNER JOIN products b
        ON a.product_id = b.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total_2,
    price,
    cumulative_quantity,
    order_date,
    LAG(quantity) OVER (PARTITION BY product_id ORDER BY price, id) AS prev_quantity,
    LEAD(quantity) OVER (PARTITION BY product_id ORDER BY price, id) AS next_quantity,
    LEAD(stock_qty) OVER (PARTITION BY product_id ORDER BY price, id) AS next_stock_qty,
    LAG(stock_qty) OVER (PARTITION BY product_id ORDER BY price, id) AS prev_stock_qty,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price, id) AS row_seq,
    LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date), product_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost
FROM ranked

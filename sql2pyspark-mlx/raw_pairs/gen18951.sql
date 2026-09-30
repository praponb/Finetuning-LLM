WITH joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.shipping_fee,
        o.order_date,
        o.status
    FROM order_items src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.order_date,
        src.status,
        p.price
    FROM joined src
    LEFT JOIN products p
        ON src.product_id = p.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT order_id
    FROM with_dims
    INTERSECT
    SELECT order_id
    FROM order_items
    WHERE order_id IN (15, 37)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
),
joined_order_items AS (
    -- Attach orders attributes
    SELECT e.*, b.status AS order_status
    FROM reconciled e
    INNER JOIN orders b
        ON e.order_id = b.id
),
joined_order_items_2 AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.stock_qty,
        b.category,
        b.cost
    FROM joined_order_items src
    LEFT JOIN products b
        ON src.product_id = b.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        category,
        DATE_TRUNC('WEEK', order_date) AS week_start,
        PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count,
        COUNT(*) AS event_count,
        GROUPING(category) AS grouping_level
    FROM joined_order_items_2
    GROUP BY GROUPING SETS ((category, DATE_TRUNC('WEEK', order_date)), (category), ())
    HAVING event_count < 1000
)
SELECT
    category,
    week_start,
    price_quartiles,
    completed_count,
    event_count,
    grouping_level,
    CASE WHEN category RLIKE '^(Sports|Electronics)$' THEN 'GROUP_B' WHEN category RLIKE '^(Home|Books)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS category_zone
FROM totals

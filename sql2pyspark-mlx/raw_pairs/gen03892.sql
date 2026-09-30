WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT order_id
    FROM cleaned
    INTERSECT ALL
    SELECT order_id
    FROM order_items
    WHERE quantity <= 4
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.order_id = k.order_id
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, d.amount
    FROM verified e
    INNER JOIN orders d
        ON e.order_id = d.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT a.*, p.amount AS order_amount
    FROM joined a
    INNER JOIN orders p
        ON a.order_id = p.id
),
with_dims_order_items AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.quantity,
        src.amount,
        b.channel
    FROM with_dims src
    LEFT JOIN orders b
        ON src.order_id = b.id
    WHERE b.customer_id IS NULL
)
-- Grouped metrics
SELECT
    order_id,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS quantity_per_mobile,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS quantity_per_web,
    AVG(amount) AS avg_amount
FROM with_dims_order_items
GROUP BY order_id

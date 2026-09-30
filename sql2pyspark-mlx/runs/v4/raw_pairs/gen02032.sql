WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_level,
        CASE WHEN unit_price >= 310 THEN 'High' WHEN unit_price >= 190 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.launch_date,
        p.stock_qty
    FROM ranked e
    LEFT JOIN products p
        ON e.product_id = p.id
),
valid_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        unit_price_level,
        LAST_DAY(launch_date) AS launch_date_month_end
    FROM joined
    WHERE order_id = 32
)
-- Multi-level aggregation
SELECT
    product_id,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    COUNT(*) AS event_count,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT product_id) AS distinct_product_ids_2,
    MIN(quantity) AS min_quantity
FROM valid_order_items
GROUP BY product_id;

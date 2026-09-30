WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(quantity) OVER w_ordered AS running_count_quantity,
        AVG(quantity) OVER w_ordered AS running_avg_quantity,
        SUM(quantity) OVER w_ordered AS running_sum_quantity,
        MIN(quantity) OVER w_ordered AS running_min_quantity,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity, id) AS quantity_bucket
    FROM order_items
    WHERE quantity BETWEEN 4 AND 6
    WINDOW w_ordered AS (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        running_sum_quantity,
        running_min_quantity,
        quantity_bucket,
        GREATEST(quantity_bucket, running_avg_quantity) AS greatest_quantity_bucket_running_avg_quantity,
        CASE WHEN unit_price >= 420 THEN 'High' WHEN unit_price >= 150 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM metrics
    WHERE product_id IS NOT NULL
      AND product_id IS NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM filtered
    INTERSECT
    SELECT product_id
    FROM order_items
    WHERE order_id IN (10, 36, 40)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.product_id = k.product_id
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.running_sum_quantity,
        src.running_min_quantity,
        src.quantity_bucket,
        src.greatest_quantity_bucket_running_avg_quantity,
        p.stock_qty,
        p.price
    FROM verified src
    LEFT JOIN products p
        ON src.product_id = p.id
)
SELECT
    s.id,
    s.product_id,
    s.running_sum_quantity,
    s.running_min_quantity,
    s.quantity_bucket,
    s.greatest_quantity_bucket_running_avg_quantity,
    s.stock_qty,
    s.price,
    (SELECT MAX(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS max_store_sale_units
FROM joined s

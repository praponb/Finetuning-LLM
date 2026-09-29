WITH with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.channel,
        x.order_item_quantity,
        x.order_item_unit_price
    FROM orders s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        status,
        order_item_unit_price,
        AVG(order_item_quantity) OVER (ORDER BY order_item_quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS order_item_quantity_avg_5,
        NTILE(5) OVER (PARTITION BY channel ORDER BY order_item_unit_price DESC, id) AS order_item_unit_price_bucket,
        ROUND(order_item_quantity / SUM(order_item_quantity) OVER (PARTITION BY customer_id), 4) AS order_item_quantity_share,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY order_item_unit_price DESC, id) AS order_item_unit_price_bucket_2,
        PERCENT_RANK() OVER (ORDER BY order_item_unit_price DESC NULLS FIRST) AS order_item_unit_price_percent_rank
    FROM with_last
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY order_item_unit_price_bucket NULLS LAST, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        order_item_unit_price,
        order_item_unit_price_bucket,
        order_item_quantity_share,
        order_item_unit_price_bucket_2,
        order_item_unit_price_percent_rank,
        CAST(row_num AS DOUBLE) AS row_num_str
    FROM ranked
    WHERE order_item_unit_price_percent_rank > 0.58
)
-- Grouped metrics
SELECT
    CASE WHEN order_item_unit_price >= 320 THEN 'HIGH' ELSE 'LOW' END AS order_item_unit_price_tier,
    customer_id,
    AVG(order_item_unit_price_bucket_2) AS avg_order_item_unit_price_bucket_2,
    ROUND(AVG(order_item_quantity_share), 2) AS avg_order_item_quantity_share,
    MIN(row_num_str) AS min_row_num_str,
    APPROX_PERCENTILE(order_item_quantity_share, 0.5) AS order_item_quantity_share_p50,
    GROUPING(CASE WHEN order_item_unit_price >= 320 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM cleaned
GROUP BY CUBE (CASE WHEN order_item_unit_price >= 320 THEN 'HIGH' ELSE 'LOW' END, customer_id)
HAVING min_row_num_str < 1000000

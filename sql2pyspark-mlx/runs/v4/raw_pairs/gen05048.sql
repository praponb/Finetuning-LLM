WITH kept_keys AS (
    -- Set difference on keys
    SELECT product_id, order_id
    FROM order_items
    EXCEPT DISTINCT
    SELECT product_id, order_id
    FROM order_items
    WHERE order_id IN (2, 14, 18, 27)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    JOIN kept_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(quantity) OVER w_part AS product_id_min_quantity,
        CUME_DIST() OVER w_recent AS unit_price_cume_dist,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id) AS corr_unit_price_quantity,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id), 4) AS quantity_share,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY unit_price NULLS FIRST) AS unit_price_cume_dist_2
    FROM verified
    WINDOW w_part AS (PARTITION BY product_id, order_id), w_recent AS (PARTITION BY order_id, product_id ORDER BY unit_price DESC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(quantity_share) OVER (PARTITION BY order_id) AS order_id_avg_quantity_share,
        SUM(unit_price_cume_dist_2) OVER (PARTITION BY order_id) AS order_id_sum_unit_price_cume_dist_2,
        MIN(unit_price_cume_dist) OVER (PARTITION BY product_id) AS product_id_min_unit_price_cume_dist,
        MAX(product_id_min_quantity) OVER (PARTITION BY product_id) AS product_id_max_product_id_min_quantity,
        RANK() OVER (ORDER BY corr_unit_price_quantity NULLS FIRST) AS corr_unit_price_quantity_rank
    FROM metrics
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.status,
        b.order_date
    FROM with_windows src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
enriched_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(unit_price) OVER w_grp AS status_sum_unit_price,
        ROUND(product_id_min_quantity / SUM(product_id_min_quantity) OVER (PARTITION BY product_id, status), 4) AS product_id_min_quantity_share,
        MAX(product_id_min_quantity) OVER (PARTITION BY product_id, order_id) AS product_id_max_product_id_min_quantity_2
    FROM enriched
    WINDOW w_grp AS (PARTITION BY status, order_id)
),
enriched_order_items_2 AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.channel,
        b.discount
    FROM enriched_order_items e
    LEFT JOIN orders b
        ON e.order_id = b.id
)
-- Grouped metrics
SELECT
    order_id,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
    SUM(order_id_sum_unit_price_cume_dist_2) FILTER (WHERE status = 'RETURNED') AS returned_total_order_id_sum_unit_price_cume_dist_2,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    GROUPING_ID() AS grouping_level
FROM enriched_order_items_2
GROUP BY CUBE (order_id, DATE_TRUNC('QUARTER', order_date))
ORDER BY quarter_start NULLS FIRST;

WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity,
        MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity
    FROM order_items
),
scored_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        AVG(running_max_quantity) OVER (PARTITION BY order_id) AS order_id_avg_running_max_quantity
    FROM scored
    WHERE running_max_quantity BETWEEN 5.2 AND 5.6
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.order_date,
        p.discount,
        p.channel
    FROM scored_order_items s
    INNER JOIN orders p
        ON s.order_id = p.id
),
scored_order_items_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY channel), 4) AS quantity_share,
        FIRST_VALUE(running_min_quantity) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY first_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_min_quantity,
        LAST_VALUE(order_id_avg_running_max_quantity, TRUE) OVER w_hist AS last_known_order_id_avg_running_max_quantity,
        NTILE(5) OVER w_hist_2 AS running_min_quantity_bucket
    FROM joined
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY MONTH(order_date), order_id ORDER BY running_min_quantity DESC, id)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, running_max_quantity / NULLIF(quantity_share, 0) AS running_max_quantity_per_quantity_share
    FROM scored_order_items_2
    WHERE unit_price >= 340
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_share DESC NULLS LAST, id) = 1
),
ranked_order_items AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, channel ORDER BY running_min_quantity DESC NULLS FIRST, id) AS rn
    FROM ranked
    QUALIFY rn = 1
)
-- Join a dimension and aggregate
SELECT
    p.status,
    e.channel AS order_item_channel,
    COUNT(*) AS rows_total,
    MAX(e.running_sum_quantity) AS max_running_sum_quantity,
    GROUPING_ID() AS level_id
FROM ranked_order_items e
INNER JOIN orders p ON e.order_id = p.id
GROUP BY GROUPING SETS ((p.status, e.channel), (p.status), ())
ORDER BY status NULLS LAST

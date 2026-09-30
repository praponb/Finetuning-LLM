WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        COALESCE(discount, 0) AS discount_filled
    FROM orders
    WHERE channel IS NOT DISTINCT FROM 'partner'
      AND customer_id IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= s.order_date
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY channel ORDER BY amount DESC NULLS FIRST) AS rn
    FROM not_flagged
    QUALIFY rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        order_date,
        status,
        amount,
        rn,
        COUNT(amount) OVER w_recent AS last3_count_amount,
        AVG(amount) OVER w_recent AS last3_avg_amount,
        SUM(amount) OVER w_recent AS last3_sum_amount,
        MIN(amount) OVER w_recent AS last3_min_amount,
        LAST_VALUE(channel) IGNORE NULLS OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM latest
    WINDOW w_recent AS (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.order_item_quantity,
        latest.unit_price
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        amount,
        rn,
        last3_avg_amount,
        last3_sum_amount,
        last3_min_amount,
        order_item_quantity,
        unit_price,
        MAX(last3_sum_amount) OVER w_part AS running_max_last3_sum_amount,
        MIN(last3_sum_amount) OVER w_part AS running_min_last3_sum_amount,
        COUNT(last3_sum_amount) OVER w_part AS running_count_last3_sum_amount
    FROM latest_match
    WINDOW w_part AS (PARTITION BY store_id ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.store_id,
    s.rn,
    s.last3_avg_amount,
    s.last3_min_amount,
    s.order_item_quantity,
    s.unit_price,
    s.running_max_last3_sum_amount,
    s.running_count_last3_sum_amount,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS avg_inventory_quantity
FROM with_windows s
ORDER BY order_item_quantity NULLS FIRST;

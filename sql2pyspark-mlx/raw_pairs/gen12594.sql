WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(discount) discount_whole,
        COALESCE(channel, 'DIRECT') resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-10-04'
      AND store_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        resolved_channel
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
deduped_orders AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee NULLS FIRST, id) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC NULLS LAST, id) AS row_num
        FROM deduped_orders
    ) x
    WHERE row_num = 1
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.order_item_quantity,
        x.order_item_unit_price
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date < s.order_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        discount,
        shipping_fee,
        resolved_channel,
        row_num,
        order_item_unit_price,
        LEAD(order_item_unit_price) OVER (PARTITION BY status ORDER BY order_item_quantity, id) next_order_item_unit_price,
        LAG(order_item_unit_price) OVER (PARTITION BY status ORDER BY order_item_quantity, id) prev_order_item_unit_price,
        LAG(order_item_quantity) OVER (PARTITION BY status ORDER BY order_item_quantity, id) prev_order_item_quantity,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_item_quantity, id) row_seq
    FROM eligible
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    ROUND(SUM(next_order_item_unit_price) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS next_order_item_unit_price_per_store,
    COUNT(*) AS row_count
FROM enriched
GROUP BY CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END
HAVING row_count < 1000
ORDER BY next_order_item_unit_price_per_store

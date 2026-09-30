WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_recent row_seq,
        LEAD(amount, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', order_date), customer_id ORDER BY shipping_fee, id) next_amount,
        FIRST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY channel, customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_shipping_fee,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY status, customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount
    FROM orders
    WINDOW w_recent AS (PARTITION BY store_id, channel ORDER BY shipping_fee, id)
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.shipment_cost,
        x.shipment_shipped_date
    FROM enriched s
    JOIN LATERAL (
            SELECT c.cost AS shipment_cost, c.shipped_date AS shipment_shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < s.order_date
        )
),
with_latest_orders AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.order_date,
        s.discount,
        s.row_seq,
        s.first_known_amount,
        x.order_item_quantity,
        x.unit_price
    FROM eligible s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_latest_orders
    EXCEPT
    SELECT id
    FROM orders
    WHERE status = 'PENDING'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest_orders s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_amount >= 1620 THEN 'HIGH' ELSE 'LOW' END AS first_known_amount_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids_3,
    ROUND(AVG(discount), 2) AS avg_discount
FROM kept_rows
GROUP BY 1
HAVING COUNT(DISTINCT id) >= 1
ORDER BY cnt DESC

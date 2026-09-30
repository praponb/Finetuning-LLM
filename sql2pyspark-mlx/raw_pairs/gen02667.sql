WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        status,
        channel,
        discount,
        shipping_fee,
        DATEDIFF(CURRENT_DATE(), order_date) days_since_order_date,
        ADD_MONTHS(order_date, 12) order_date_plus_months
    FROM orders
    WHERE store_id IN (12, 19)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(discount) OVER w_hist prev_discount,
        LEAD(shipping_fee) OVER w_hist next_shipping_fee,
        ROW_NUMBER() OVER w_hist row_seq,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY days_since_order_date ASC, id ASC) days_since_order_date_bucket
    FROM filtered
    WINDOW w_hist AS (PARTITION BY customer_id, status ORDER BY discount ASC, id ASC)
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.status,
        s.discount,
        s.shipping_fee,
        s.prev_discount,
        s.next_shipping_fee,
        s.days_since_order_date_bucket,
        x.unit_price,
        x.quantity
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        prev_discount,
        next_shipping_fee,
        unit_price,
        quantity
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY prev_discount DESC) AS rank_in_group
        FROM with_latest
    ) t
    WHERE rank_in_group = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.cost <= 42
        )
),
report AS (
    SELECT
        id,
        prev_discount,
        quantity
    FROM eligible
    ORDER BY prev_discount DESC NULLS LAST
)
SELECT
    id,
    prev_discount,
    quantity
FROM report;

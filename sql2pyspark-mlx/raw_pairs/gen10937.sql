WITH with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.status,
        s.amount,
        latest.quantity,
        latest.order_item_unit_price
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    SELECT customer_id
    FROM with_latest
    EXCEPT ALL
    SELECT customer_id
    FROM orders
    WHERE status <> 'RETURNED'
),
kept_rows AS (
    SELECT s.*
    FROM with_latest s
    JOIN kept_keys k ON s.customer_id = k.customer_id
),
cleaned AS (
    SELECT *, amount * order_item_unit_price AS amount_times_order_item_unit_price
    FROM kept_rows
    WHERE customer_id IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at <= s.order_date
        )
),
scored AS (
    SELECT
        *,
        STDDEV_SAMP(amount_times_order_item_unit_price) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_amount_times_order_item_unit_price,
        ROUND(order_item_unit_price / SUM(order_item_unit_price) OVER (PARTITION BY customer_id, status), 4) AS order_item_unit_price_share
    FROM not_flagged
)
SELECT
    customer_id,
    metric_name,
    val
FROM (
    SELECT
        customer_id,
        order_item_unit_price,
        amount,
        amount_times_order_item_unit_price
    FROM scored
) src
UNPIVOT (
    val FOR metric_name IN (order_item_unit_price AS ORDER_ITEM_UNIT_PRICE_VAL, amount AS AMOUNT, amount_times_order_item_unit_price AS AMOUNT_TIMES_ORDER_ITEM_UNIT_PRICE)
);

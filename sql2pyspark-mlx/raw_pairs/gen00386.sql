WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        LEAST(amount, shipping_fee) AS least_amount_shipping_fee,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM orders
    WHERE order_date >= DATE '2023-03-22'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.method NOT IN ('bank_transfer', 'card', 'cash')
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.order_id = 39
        )
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.order_date,
        s.status,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.least_amount_shipping_fee,
        s.channel_clean,
        latest.payment_amount
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(payment_amount) OVER w_hist AS payment_amount_avg_6,
        MAX(discount) OVER (PARTITION BY YEAR(order_date)) AS order_date_year_max_discount
    FROM with_last
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('MONTH', order_date), store_id ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.store_id,
    s.amount,
    s.channel_clean,
    (SELECT SUM(i2.quantity) FROM inventory i2 WHERE i2.store_id = s.store_id) AS sum_inventory_quantity
FROM with_windows s;

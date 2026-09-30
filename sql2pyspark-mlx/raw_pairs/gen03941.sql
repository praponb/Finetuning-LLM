WITH with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.channel,
        x.unit_price
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS row_seq,
        DENSE_RANK() OVER (PARTITION BY customer_id, channel ORDER BY unit_price DESC NULLS LAST) AS unit_price_dense_rank
    FROM with_last
    WHERE unit_price < 170
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at < s.order_date
        )
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM not_flagged e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.order_date BETWEEN sub.start_date AND sub.end_date
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        plan,
        channel,
        unit_price_dense_rank
    FROM with_window
) src
PIVOT (
    SUM(unit_price_dense_rank) AS amount,
    COUNT(1) AS n
    FOR channel IN ('mobile', 'partner')
);

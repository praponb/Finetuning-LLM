WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM orders e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.order_date BETWEEN sub.start_date AND sub.end_date
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.store_id,
        a.discount,
        a.subscription_id,
        a.plan,
        a.monthly_fee,
        p.city,
        p.segment
    FROM attributed a
    LEFT JOIN customers p
        ON a.customer_id = p.id
    WHERE p.age = 44
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.discount,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        s.segment,
        x.payment_paid_at,
        x.payment_amount
    FROM joined s
    JOIN LATERAL (
            SELECT c.paid_at AS payment_paid_at, c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at < s.payment_paid_at
        )
),
latest_match_orders AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.discount,
        s.subscription_id,
        s.segment,
        x.status
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        subscription_id,
        segment,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, status ORDER BY discount, id, subscription_id) AS latest_rank
        FROM latest_match_orders
    ) ranked
    WHERE latest_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.quantity <= 4
        )
)
-- Aggregate with conditional metrics
SELECT
    subscription_id,
    ROUND(SUM(subscription_id) / NULLIF(COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END), 0), 2) AS subscription_id_per_refunded,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2,
    COUNT(*) AS event_count
FROM kept
GROUP BY subscription_id
HAVING COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) >= 1
ORDER BY subscription_id_per_refunded NULLS LAST;

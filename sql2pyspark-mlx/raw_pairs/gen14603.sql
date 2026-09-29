WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        shipping_fee * amount AS amount_with_shipping
    FROM orders
    WHERE channel = 'store'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        amount_with_shipping
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, customer_id ORDER BY shipping_fee NULLS FIRST, id) = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.unit_price BETWEEN 280 AND 320
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY amount DESC NULLS LAST, id) AS dedup_rank
    FROM not_flagged
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.shipping_fee,
    s.amount_with_shipping,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM deduped s
ORDER BY amount_with_shipping NULLS LAST

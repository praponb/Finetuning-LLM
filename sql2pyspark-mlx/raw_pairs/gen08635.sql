WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(order_date, 'yyyy-MM-dd') order_date_label,
        COALESCE(channel, 'DIRECT') resolved_channel
    FROM orders
    WHERE customer_id IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'web'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, amount + discount amount_plus_discount
    FROM valid
    WHERE order_date > DATE '2023-02-26'
      AND discount BETWEEN 0.12 AND 0.37
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC, id) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at <= s.order_date
        )
)
SELECT
    id,
    customer_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    order_date_label
FROM eligible

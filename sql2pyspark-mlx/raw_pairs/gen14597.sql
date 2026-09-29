WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-05-19 07:00:00'
      AND method IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.status,
        a.amount_filled,
        o.discount
    FROM base a
    INNER JOIN orders o
        ON a.order_id <=> o.id
    WHERE o.channel NOT IN ('web', 'mobile', 'store')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY amount_filled) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.discount,
        p.shipping_fee,
        p.channel,
        p.amount
    FROM latest s
    INNER JOIN orders p
        ON s.order_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS rn
    FROM joined
    QUALIFY rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        channel,
        amount,
        rn,
        NTILE(4) OVER (PARTITION BY channel ORDER BY amount, id) AS amount_bucket,
        RANK() OVER (PARTITION BY channel, order_id ORDER BY amount DESC NULLS LAST) AS amount_rank
    FROM ranked
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY order_id, channel ORDER BY rn, id) AS prev_amount,
        LAG(amount_bucket) OVER (PARTITION BY order_id, channel ORDER BY rn, id) AS prev_amount_bucket,
        LEAD(amount_bucket) OVER (PARTITION BY order_id, channel ORDER BY rn, id) AS next_amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id, channel ORDER BY rn, id) AS row_seq
    FROM scored
)
SELECT
    id,
    order_id,
    channel,
    amount,
    rn,
    amount_bucket,
    amount_rank,
    prev_amount,
    prev_amount_bucket
FROM enriched
ORDER BY amount DESC NULLS LAST;

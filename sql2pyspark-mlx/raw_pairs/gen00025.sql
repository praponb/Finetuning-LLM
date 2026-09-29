WITH enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.shipping_fee,
        p.amount AS order_amount,
        p.channel
    FROM payments src
    LEFT JOIN orders p
        ON src.order_id = p.id
    WHERE p.store_id IS NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        order_amount,
        channel
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM ranked
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        channel,
        SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        LAST_VALUE(amount) IGNORE NULLS OVER (ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        LEAD(session_id, 2) OVER (PARTITION BY channel, order_id ORDER BY paid_at ASC, id ASC) AS next_session_id,
        AVG(session_id) OVER (PARTITION BY channel ORDER BY session_id ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS session_id_avg_2
    FROM islands
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.last_known_amount,
        o.discount
    FROM metrics a
    LEFT JOIN orders o
        ON a.order_id <=> o.id
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    discount,
    FIRST_VALUE(last_known_amount) IGNORE NULLS OVER (ORDER BY last_known_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_amount,
    SUM(last_known_amount) OVER (PARTITION BY order_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_amount,
    MIN(discount) OVER (PARTITION BY order_id) AS order_id_min_discount
FROM with_dims;

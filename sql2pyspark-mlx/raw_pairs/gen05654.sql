WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        CASE WHEN status IN ('FAILED', 'REFUNDED') THEN 'Priority' ELSE 'Other' END AS status_group
    FROM payments
    WHERE paid_at >= DATE '2023-11-13'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.shipping_fee,
        p.status AS order_status,
        p.order_date
    FROM cleaned e
    JOIN orders p
        ON e.order_id = p.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.status_group,
        src.shipping_fee,
        o.channel,
        o.amount AS order_amount
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
enriched_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_group,
        shipping_fee,
        channel,
        LAG(order_amount) OVER (PARTITION BY channel ORDER BY order_amount, id) AS prev_order_amount,
        LEAD(order_amount) OVER (PARTITION BY channel ORDER BY order_amount, id) AS next_order_amount,
        LAG(shipping_fee) OVER (PARTITION BY channel ORDER BY order_amount, id) AS prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_amount, id) AS seq_no,
        AVG(order_amount) OVER (PARTITION BY channel ORDER BY order_amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS order_amount_avg_5
    FROM joined
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        status_group,
        shipping_fee,
        prev_order_amount,
        prev_shipping_fee
    FROM enriched_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC NULLS LAST, id) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(prev_order_amount) AS prev_order_amount_whole
    FROM latest
    WHERE prev_shipping_fee IS NOT NULL
),
enriched_payments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(prev_order_amount_whole) IGNORE NULLS OVER (ORDER BY prev_order_amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_order_amount_whole,
        ROW_NUMBER() OVER (ORDER BY prev_shipping_fee, id) AS row_seq
    FROM valid
)
SELECT
    id,
    status_group,
    shipping_fee,
    prev_order_amount,
    prev_shipping_fee,
    prev_order_amount_whole,
    last_known_prev_order_amount_whole,
    row_seq
FROM enriched_payments_2
ORDER BY prev_shipping_fee DESC

WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN channel = 'web' THEN 1 ELSE 0 END AS is_web
    FROM orders
    WHERE order_date > DATE '2023-06-23'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY shipping_fee ASC, id ASC) AS row_seq,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY channel ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        SUM(shipping_fee) OVER w_recent AS running_shipping_fee,
        NTILE(10) OVER (PARTITION BY status ORDER BY discount ASC, id ASC) AS discount_bucket,
        SUM(is_web) OVER (PARTITION BY MONTH(order_date) ORDER BY is_web ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_web
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        discount,
        last_known_amount,
        running_shipping_fee,
        discount_bucket,
        running_is_web,
        LEAD(running_shipping_fee) OVER w_part AS next_running_shipping_fee,
        LAG(amount) OVER w_part AS prev_amount,
        LEAD(amount) OVER w_part AS next_amount,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM ranked
    WINDOW w_part AS (PARTITION BY store_id ORDER BY running_shipping_fee ASC, id ASC)
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.discount,
        s.last_known_amount,
        s.running_shipping_fee,
        s.prev_amount,
        s.next_amount,
        latest.status
    FROM metrics s
    JOIN LATERAL (
            SELECT c.status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    discount,
    last_known_amount,
    running_shipping_fee,
    next_amount
FROM with_latest;

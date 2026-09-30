WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(shipping_fee) OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS prev_shipping_fee,
        LEAD(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS next_discount,
        LAG(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS prev_discount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS event_seq,
        LAST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee
    FROM orders
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(event_seq * 0.85, 2) AS event_seq_adjusted,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM scored
    WHERE store_id IS NOT NULL
      AND store_id = 39
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(prev_discount) OVER w_grp AS prev_prev_discount,
        LEAD(prev_discount) OVER w_grp AS next_prev_discount,
        LEAD(amount) OVER w_grp AS next_amount,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM valid
    WINDOW w_grp AS (PARTITION BY status, channel ORDER BY last_known_shipping_fee, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_prev_discount DESC, id) AS seq_no,
        RANK() OVER (PARTITION BY status, store_id ORDER BY next_prev_discount DESC) AS next_prev_discount_rank,
        SUM(row_seq) OVER (PARTITION BY YEAR(order_date), status ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq
    FROM with_windows
    WHERE discount < 0.26
)
SELECT
    id,
    order_date,
    status,
    channel,
    discount,
    prev_shipping_fee,
    next_discount,
    prev_discount,
    event_seq,
    CASE WHEN status RLIKE '^(PENDING|CANCELLED)$' THEN 'GROUP_A' WHEN status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS status_zone
FROM metrics
ORDER BY id DESC NULLS FIRST, status_zone;

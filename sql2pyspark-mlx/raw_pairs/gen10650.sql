WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        status,
        LENGTH(status) AS status_length
    FROM shipments
    WHERE delivered_date >= DATE '2023-07-15'
      AND status IN ('LOST', 'IN_TRANSIT', 'DELIVERED')
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.status_length,
        d.channel,
        d.order_date,
        d.shipping_fee
    FROM valid s
    JOIN orders d
        ON s.order_id <=> d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        order_date,
        shipping_fee,
        AVG(shipping_fee) OVER (PARTITION BY DATE_TRUNC('YEAR', order_date), order_id ORDER BY status_length, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date, id) AS seq_no,
        MAX(status_length) OVER (PARTITION BY order_id) AS order_id_max_status_length
    FROM enriched
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_date,
        shipping_fee_avg_3_prior,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        LEAD(order_id_max_status_length, 2) OVER (PARTITION BY DATE_TRUNC('YEAR', order_date), order_id ORDER BY shipping_fee, id) AS next_order_id_max_status_length,
        LEAD(order_id_max_status_length, 2) OVER w_running AS next_order_id_max_status_length_2
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY shipping_fee, id), w_running AS (PARTITION BY order_id ORDER BY order_date, id)
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(shipping_fee_avg_3_prior) OVER w_part AS next_shipping_fee_avg_3_prior,
        LAG(shipping_fee_avg_3_prior) OVER w_part AS prev_shipping_fee_avg_3_prior,
        LAG(event_seq) OVER w_part AS prev_event_seq,
        LEAD(event_seq) OVER w_part AS next_event_seq,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM metrics
    WINDOW w_part AS (ORDER BY next_order_id_max_status_length, id)
)
SELECT
    id,
    order_date,
    shipping_fee_avg_3_prior,
    event_seq,
    next_order_id_max_status_length,
    next_order_id_max_status_length_2,
    next_shipping_fee_avg_3_prior,
    prev_shipping_fee_avg_3_prior,
    prev_event_seq
FROM with_windows_shipments;

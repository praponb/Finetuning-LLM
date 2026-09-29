WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', status, method) status_method_key,
        ROUND(amount * 1.07, 2) adjusted_amount
    FROM payments
    WHERE paid_at > TO_DATE('2023-06-23')
      AND method IS DISTINCT FROM 'paypal'
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.method,
        src.amount,
        src.status,
        src.status_method_key,
        b.discount,
        b.order_date,
        b.status AS order_status
    FROM filtered src
    LEFT JOIN orders b
        ON src.order_id <=> b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER w_hist running_sum_amount,
        MIN(amount) OVER w_hist running_min_amount,
        MAX(amount) OVER w_hist running_max_amount,
        COUNT(amount) OVER w_hist running_count_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount DESC, id) event_seq
    FROM joined
    WHERE method = 'bank_transfer'
    WINDOW w_hist AS (PARTITION BY order_status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.channel,
        o.shipping_fee,
        o.amount AS order_amount
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
)
-- Window metrics over several different partitions
SELECT
    id,
    method,
    amount,
    status_method_key,
    order_status,
    running_min_amount,
    running_max_amount,
    running_count_amount,
    channel,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_min_amount DESC, id) event_seq_2,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST) amount_dense_rank
FROM enriched_payments;

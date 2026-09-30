WITH cleaned AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        TRIM(method) clean_method
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-09-14 08:00:00'
      AND order_id = 6
),
with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM cleaned
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_payments AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        clean_method,
        SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
scored AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY burst_id DESC, id) burst_id_bucket,
        AVG(burst_id) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) burst_id_avg_6_prior
    FROM sessions_payments
),
with_dims AS (
    SELECT
        a.*,
        b.shipping_fee,
        b.order_date,
        b.channel
    FROM scored a
    INNER JOIN orders b
        ON a.order_id = b.id
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY shipping_fee DESC NULLS LAST, id) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
with_windows AS (
    SELECT
        *,
        MIN(burst_id_avg_6_prior) OVER (PARTITION BY status, method ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_burst_id_avg_6_prior,
        COUNT(burst_id_avg_6_prior) OVER (PARTITION BY status, method ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_burst_id_avg_6_prior,
        SUM(burst_id_avg_6_prior) OVER (PARTITION BY status, method ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_burst_id_avg_6_prior
    FROM deduped
)
SELECT
    o.channel AS order_channel,
    src.status AS payment_status,
    COUNT(*) AS rows_total,
    SUM(src.running_min_burst_id_avg_6_prior) AS total_running_min_burst_id_avg_6_prior,
    GROUPING_ID() AS level_id
FROM with_windows src
INNER JOIN orders o ON src.order_id = o.id
GROUP BY CUBE (o.channel, src.status);

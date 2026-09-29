WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.amount,
        e.status,
        o.channel,
        o.shipping_fee,
        o.status AS order_status
    FROM payments e
    INNER JOIN orders o
        ON e.order_id = o.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        order_status,
        LAST_VALUE(order_status) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_status,
        NTILE(5) OVER (PARTITION BY method, channel ORDER BY amount, id) AS amount_bucket,
        NTILE(5) OVER (PARTITION BY method ORDER BY shipping_fee, id) AS shipping_fee_bucket
    FROM with_dims
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_status,
        last_known_order_status,
        amount_bucket,
        LEAD(shipping_fee_bucket) OVER w_hist AS next_shipping_fee_bucket,
        LEAD(amount_bucket) OVER w_hist AS next_amount_bucket,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM enriched
    WINDOW w_hist AS (PARTITION BY order_status, method ORDER BY amount_bucket, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_status ORDER BY amount_bucket DESC NULLS LAST, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (ORDER BY amount_bucket, id) AS amount_bucket_bucket,
        SUM(seq_no) OVER (ORDER BY next_amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        AVG(next_shipping_fee_bucket) OVER (PARTITION BY order_status ORDER BY next_shipping_fee_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next_shipping_fee_bucket_avg_2_prior,
        AVG(seq_no) OVER (PARTITION BY order_status ORDER BY amount_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS seq_no_avg_2_prior
    FROM deduped
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_status,
        last_known_order_status,
        amount_bucket,
        seq_no,
        amount_bucket_bucket,
        seq_no_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY order_status ORDER BY amount_bucket_bucket DESC) AS amount_bucket_bucket_dense_rank,
        LAST_VALUE(running_seq_no) IGNORE NULLS OVER (PARTITION BY order_status ORDER BY next_amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_seq_no,
        SUM(next_amount_bucket) OVER (PARTITION BY order_status) AS order_status_sum_next_amount_bucket,
        LAST_VALUE(next_amount_bucket, TRUE) OVER (ORDER BY running_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_amount_bucket
    FROM with_windows
),
enriched_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY order_status_sum_next_amount_bucket DESC, id) AS order_status_sum_next_amount_bucket_bucket,
        LEAD(seq_no_avg_2_prior, 2, 0) OVER (ORDER BY seq_no_avg_2_prior, id) AS next_seq_no_avg_2_prior,
        ROUND(last_known_next_amount_bucket / SUM(last_known_next_amount_bucket) OVER (PARTITION BY order_status), 4) AS last_known_next_amount_bucket_share
    FROM metrics
)
-- Multi-level aggregation
SELECT
    CASE WHEN seq_no_avg_2_prior >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_avg_2_prior_tier,
    last_known_running_seq_no,
    CASE WHEN amount_bucket_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS amount_bucket_bucket_tier,
    COUNT(CASE WHEN order_status = 'CANCELLED' THEN 1 END) AS cancelled_count,
    PERCENTILE_APPROX(amount_bucket_bucket_dense_rank, 0.5) AS amount_bucket_bucket_dense_rank_p50,
    SORT_ARRAY(COLLECT_SET(order_status)) AS order_status_list
FROM enriched_payments
GROUP BY seq_no_avg_2_prior_tier, last_known_running_seq_no, amount_bucket_bucket_tier
ORDER BY amount_bucket_bucket_dense_rank_p50 DESC NULLS FIRST

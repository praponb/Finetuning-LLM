WITH filtered AS (
    SELECT
        id,
        customer_id,
        status,
        amount,
        discount,
        shipping_fee,
        shipping_fee - amount amount_with_shipping,
        COALESCE(channel, 'UNKNOWN') resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-08-04'
),
metrics AS (
    SELECT
        id,
        status,
        FIRST_VALUE(amount, TRUE) OVER w_recent first_known_amount,
        ROW_NUMBER() OVER (ORDER BY shipping_fee DESC, id) event_seq,
        NTILE(4) OVER w_hist amount_bucket
    FROM filtered
    WINDOW w_recent AS (PARTITION BY status ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY status ORDER BY amount DESC, id)
),
scored AS (
    SELECT
        id,
        status,
        NTILE(5) OVER (PARTITION BY status ORDER BY first_known_amount DESC, id) first_known_amount_bucket,
        ROUND(amount_bucket / SUM(amount_bucket) OVER (PARTITION BY status), 4) amount_bucket_share
    FROM metrics
),
valid AS (
    SELECT *, ROUND(first_known_amount_bucket * 1.15, 2) adjusted_first_known_amount_bucket
    FROM scored
    WHERE amount_bucket_share >= 0.6
)
SELECT
    id,
    first_known_amount_bucket,
    amount_bucket_share,
    adjusted_first_known_amount_bucket,
    MAX(adjusted_first_known_amount_bucket) OVER w_grp running_max_adjusted_first_known_amount_bucket,
    COUNT(adjusted_first_known_amount_bucket) OVER w_grp running_count_adjusted_first_known_amount_bucket,
    SUM(adjusted_first_known_amount_bucket) OVER w_grp running_sum_adjusted_first_known_amount_bucket,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount_bucket_share DESC NULLS LAST) amount_bucket_share_percent_rank
FROM valid
WINDOW w_grp AS (PARTITION BY status ORDER BY first_known_amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

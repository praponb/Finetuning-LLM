WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.channel,
        d.amount AS order_amount
    FROM base e
    LEFT JOIN orders d
        ON e.order_id <=> d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        channel,
        order_amount,
        LEAD(amount_int) OVER (PARTITION BY order_id ORDER BY order_amount, id) AS next_amount_int,
        LAG(order_amount) OVER (PARTITION BY order_id ORDER BY order_amount, id) AS prev_order_amount,
        LEAD(order_amount) OVER (PARTITION BY order_id ORDER BY order_amount, id) AS next_order_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_amount, id) AS event_seq
    FROM with_dims
    WHERE TO_DATE(paid_at) >= '2025-07-28'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status,
        channel,
        order_amount,
        prev_order_amount,
        next_order_amount,
        event_seq,
        prev_order_amount + order_amount AS prev_order_amount_plus_order_amount,
        LEAST(next_order_amount, order_amount) AS least_next_order_amount_order_amount
    FROM with_windows
    WHERE channel IS NOT NULL
      AND channel = 'store'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(event_seq) OVER (PARTITION BY status ORDER BY least_next_order_amount_order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq,
        ROW_NUMBER() OVER (ORDER BY next_order_amount DESC, id) AS seq_no,
        STDDEV_SAMP(least_next_order_amount_order_amount) OVER (PARTITION BY channel, status) AS channel_stddev_samp_least_next_order_amount_order_amount
    FROM valid
),
with_windows_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        next_order_amount,
        prev_order_amount_plus_order_amount,
        running_event_seq,
        NTILE(5) OVER (PARTITION BY status, channel ORDER BY running_event_seq, id) AS running_event_seq_bucket,
        ROW_NUMBER() OVER (PARTITION BY status, channel ORDER BY least_next_order_amount_order_amount DESC, id) AS seq_no_2
    FROM scored
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(channel, 'unassigned') AS channel_filled,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM with_windows_payments
    WHERE channel IS NOT NULL
      AND seq_no_2 = 4
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_2_tier,
    CASE WHEN next_order_amount >= 1690 THEN 'LARGE' ELSE 'SMALL' END AS next_order_amount_tier,
    COUNT(*) FILTER (WHERE channel = 'store') AS store_rows,
    ROUND(SUM(running_event_seq_bucket) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS running_event_seq_bucket_per_store
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN seq_no_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_order_amount >= 1690 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN seq_no_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY next_order_amount_tier DESC NULLS FIRST;

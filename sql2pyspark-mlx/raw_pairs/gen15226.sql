WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(discount, 2) AS discount_rounded,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE order_date >= TO_DATE('2023-06-09')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount) OVER w_running AS next_amount,
        LAG(discount) OVER w_running AS prev_discount,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM valid
    WINDOW w_running AS (PARTITION BY channel ORDER BY amount, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        shipping_fee,
        channel_clean,
        next_amount,
        prev_discount,
        seq_no,
        LAG(amount, 1, 0) OVER (PARTITION BY channel ORDER BY seq_no, id) AS prev_amount,
        DENSE_RANK() OVER (PARTITION BY YEAR(order_date), channel ORDER BY discount_rounded) AS discount_rounded_dense_rank
    FROM scored
),
scored_orders AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel_clean,
        next_amount,
        seq_no,
        prev_amount,
        LEAD(discount_rounded_dense_rank, 1) OVER (PARTITION BY store_id ORDER BY prev_discount, id) AS next_discount_rounded_dense_rank,
        ROW_NUMBER() OVER (ORDER BY prev_amount, id) AS event_seq,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY store_id), 4) AS seq_no_share
    FROM metrics
    WHERE store_id IS NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= '2025-08-01'
        )
)
-- Multi-level aggregation
SELECT
    seq_no,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(event_seq, 0.5) AS event_seq_p50
FROM kept
GROUP BY seq_no
ORDER BY event_seq_p50 DESC NULLS LAST

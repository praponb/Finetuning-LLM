WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), order_date) / 365.25 AS years_since_order_date
    FROM orders
    WHERE order_date BETWEEN '2023-04-01' AND '2023-07-15'
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        x.paid_at
    FROM base s
    JOIN LATERAL (
            SELECT c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY channel, status) AS channel_sum_amount,
        COUNT(discount) OVER (PARTITION BY status, channel ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS discount_count_90d,
        LAG(discount, 2) OVER (PARTITION BY channel ORDER BY discount, id) AS prev_discount
    FROM with_latest
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY session_no DESC, id) AS rank_in_group
        FROM islands
    ) ranked
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.order_id IN (1, 18, 23)
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        channel,
        discount,
        paid_at,
        channel_sum_amount,
        is_new_session,
        session_no,
        rank_in_group,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY channel_sum_amount) AS channel_sum_amount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_new_session DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY rank_in_group DESC, id) AS row_seq,
        PERCENT_RANK() OVER (ORDER BY discount NULLS FIRST) AS discount_percent_rank
    FROM not_flagged
    WHERE status != 'COMPLETED'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN channel_sum_amount >= 7994 THEN 'LARGE' ELSE 'SMALL' END AS channel_sum_amount_tier,
    MAX(seq_no) AS max_seq_no,
    COUNT(*) AS row_count,
    SUM(CASE WHEN status = 'PENDING' THEN channel_sum_amount_cume_dist ELSE 0 END) AS pending_channel_sum_amount_cume_dist,
    SUM(discount_percent_rank) AS total_discount_percent_rank,
    AVG(session_no) FILTER (WHERE channel = 'store') AS store_avg_session_no
FROM metrics
GROUP BY CASE WHEN channel_sum_amount >= 7994 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) > 1
ORDER BY total_discount_percent_rank;

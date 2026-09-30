WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        WEEKOFYEAR(paid_at) AS paid_at_weekofyear,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE paid_at < '2024-04-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY amount DESC, id) AS amount_bucket,
        DENSE_RANK() OVER (PARTITION BY order_id, method ORDER BY paid_at_weekofyear DESC NULLS LAST) AS paid_at_weekofyear_dense_rank,
        FIRST_VALUE(paid_at_weekofyear) IGNORE NULLS OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_paid_at_weekofyear
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Running and moving aggregates
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY paid_at_weekofyear, id) AS seq_no,
    LEAD(new_group_flag, 1) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS next_new_group_flag,
    COUNT(first_known_paid_at_weekofyear) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS first_known_paid_at_weekofyear_count_30d,
    MAX(burst_id) OVER (PARTITION BY order_id, method) AS order_id_max_burst_id,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at_weekofyear DESC, id) AS event_seq
FROM sessions_payments

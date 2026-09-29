WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        UPPER(method) AS method_upper,
        CASE WHEN amount >= 1560 THEN 'High' WHEN amount >= 450 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        COALESCE(method, 'NONE') AS method_filled
    FROM payments
    WHERE method IS DISTINCT FROM 'paypal'
      AND HOUR(paid_at) BETWEEN 10 AND 18
),
valid_payments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        method_upper,
        amount_tier,
        method_filled,
        CAST(paid_at AS DATE) AS paid_at_day,
        COALESCE(method, 'N/A') AS method_filled_2
    FROM valid
    WHERE paid_at > TO_DATE('2023-05-23')
      AND order_id IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid_payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        method_upper,
        method_filled,
        paid_at_day,
        SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
matched_keys AS (
    -- Set difference on keys
    SELECT method, id
    FROM sessionized
    EXCEPT ALL
    SELECT method, id
    FROM payments
    WHERE order_id IN (20, 36)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN matched_keys k ON s.method = k.method AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY method, status ORDER BY session_no ASC) AS session_no_rank,
        RANK() OVER (PARTITION BY method ORDER BY session_no DESC) AS session_no_rank_2,
        RANK() OVER (ORDER BY session_no ASC) AS session_no_rank_3
    FROM reconciled
    WHERE method IS NULL
),
stats AS (
    -- Grouped metrics
    SELECT
        session_no,
        CASE WHEN session_no_rank_3 >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_no_rank_3_tier,
        ROUND(SUM(session_no_rank_3) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS session_no_rank_3_per_success,
        SUM(CASE WHEN method = 'paypal' THEN session_no_rank_3 ELSE 0 END) AS paypal_session_no_rank_3,
        MAX(paid_at) AS last_paid_at,
        GROUPING_ID() AS grouping_id
    FROM enriched
    GROUP BY GROUPING SETS ((session_no, CASE WHEN session_no_rank_3 >= 8 THEN 'HIGH' ELSE 'LOW' END), (session_no), ())
)
SELECT
    session_no,
    session_no_rank_3_tier,
    session_no_rank_3_per_success,
    last_paid_at,
    grouping_id
FROM stats
ORDER BY session_no_rank_3_per_success DESC;

WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CASE WHEN amount >= 1680 THEN 'High' WHEN amount >= 210 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        DATE_TRUNC('year', paid_at) AS paid_at_period,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE paid_at >= TO_DATE('2023-10-20')
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at_period) OVER (PARTITION BY status ORDER BY paid_at_period ASC, id ASC) AS prev_paid_at_period
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at_period IS NULL OR paid_at_period > prev_paid_at_period + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_paid_at_period) OVER (PARTITION BY method ORDER BY prev_paid_at_period ASC, id ASC) IS NULL
                 OR prev_paid_at_period > LAG(prev_paid_at_period) OVER (PARTITION BY method ORDER BY prev_paid_at_period ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY prev_paid_at_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_tier,
        paid_at_period,
        method_filled,
        prev_paid_at_period,
        is_new_burst,
        burst_id,
        new_group_flag,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY prev_paid_at_period DESC, id ASC) AS event_seq,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY method ORDER BY paid_at_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        LAG(is_new_burst, 1) OVER (PARTITION BY status ORDER BY is_new_burst ASC, id ASC) AS prev_is_new_burst
    FROM islands
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(session_id, 1, 0) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) AS prev_session_id,
        SUM(first_known_amount) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_first_known_amount,
        SUM(is_new_burst) OVER (PARTITION BY method ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_new_burst
    FROM enriched
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at_period) OVER (PARTITION BY status ORDER BY paid_at_period ASC, id ASC) AS prev_paid_at_period_2
    FROM scored
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at_period_2 IS NULL OR paid_at_period > prev_paid_at_period_2 + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    session_id,
    CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    CASE WHEN is_new_burst >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_burst_tier,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95,
    SUM(amount) FILTER (WHERE method = 'promptpay') AS promptpay_total_amount,
    COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count,
    GROUPING(session_id) AS gid
FROM sessions_payments
GROUP BY GROUPING SETS ((session_id, CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN is_new_burst >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END), (session_id, CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END), (session_id))
ORDER BY event_seq_tier DESC NULLS LAST;

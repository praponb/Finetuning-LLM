WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        ROUND(amount * 1.15, 2) AS amount_adjusted,
        CASE WHEN method = 'cash' THEN 1 ELSE 0 END AS is_cash
    FROM payments
    WHERE method IS NOT NULL
      AND method IS DISTINCT FROM 'paypal'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(is_cash) OVER w_part AS is_cash_max_30d,
        ROW_NUMBER() OVER (PARTITION BY YEAR(paid_at) ORDER BY is_cash, id) AS event_seq,
        STDDEV_SAMP(amount) OVER (PARTITION BY status) AS status_stddev_samp_amount
    FROM islands
    WHERE amount_adjusted BETWEEN 58 AND 76
    WINDOW w_part AS (PARTITION BY TO_DATE(paid_at), method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM enriched
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        paid_at,
        is_cash,
        session_no,
        is_cash_max_30d,
        event_seq,
        status_stddev_samp_amount,
        SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    id,
    paid_at,
    session_no,
    status_stddev_samp_amount,
    island_id
FROM sessions_payments;

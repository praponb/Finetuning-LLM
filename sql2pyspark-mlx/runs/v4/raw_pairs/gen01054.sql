WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessions src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at <= DATE_ADD(sub.end_date, 7)
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.duration_sec
    FROM with_window s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id, subscription_id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_last
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_30d,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY session_no DESC, id, subscription_id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY duration_sec, id, subscription_id) AS seq_no_2
    FROM islands
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        started_at,
        country,
        subscription_id,
        plan,
        monthly_fee,
        duration_sec,
        session_start_flag,
        monthly_fee_avg_30d,
        seq_no,
        seq_no_2,
        UPPER(country) AS country_upper,
        monthly_fee_avg_30d - seq_no AS monthly_fee_avg_30d_minus_seq_no
    FROM enriched
    WHERE started_at >= TIMESTAMP '2023-03-25 03:00:00'
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.web_event_event_time,
        top1.web_event_event_type
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device,
        started_at,
        country,
        subscription_id,
        plan,
        monthly_fee,
        duration_sec,
        session_start_flag,
        monthly_fee_avg_30d,
        seq_no,
        seq_no_2,
        country_upper,
        monthly_fee_avg_30d_minus_seq_no,
        web_event_event_time,
        CASE WHEN web_event_event_type = 'purchase' THEN 1 ELSE 0 END AS is_purchase,
        COALESCE(plan, 'DIRECT') AS resolved_plan
    FROM latest_match
    WHERE started_at > TO_TIMESTAMP('2023-04-16 16:00:00')
)
-- Multi-level aggregation
SELECT
    plan,
    device,
    CASE WHEN seq_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_tier,
    ROUND(SUM(is_purchase) / NULLIF(COUNT(CASE WHEN country = 'UK' THEN 1 END), 0), 2) AS is_purchase_per_uk,
    APPROX_PERCENTILE(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    (UNIX_TIMESTAMP(MAX(web_event_event_time)) - UNIX_TIMESTAMP(MIN(web_event_event_time))) / 60 AS span_minutes,
    PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    COUNT(*) AS row_count
FROM valid
GROUP BY plan, device, CASE WHEN seq_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY row_count NULLS FIRST

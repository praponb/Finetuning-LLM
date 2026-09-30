WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE channel = 'phone'
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        channel,
        burst_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, burst_id
),
totals AS (
    -- Group-level totals
    SELECT
        channel,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        MIN(session_end) AS first_session_end,
        SUM(CASE WHEN channel NOT IN ('email', 'chat') THEN burst_id ELSE 0 END) AS burst_id_channel_not_in_email_chat,
        ROUND(100 * COUNT(CASE WHEN channel IN ('phone', 'chat', 'email') THEN 1 END) / COUNT(*), 1) AS pct_channel_in_phone_chat_email,
        PERCENTILE_APPROX(burst_id, 0.5) AS median_burst_id,
        ROUND(SUM(event_count) / NULLIF(SUM(burst_id), 0), 3) AS event_count_to_burst_id_ratio,
        SUM(CASE WHEN channel NOT IN ('web', 'email') THEN 1 ELSE 0 END) AS n_channel_not_in_web_email
    FROM session_summary
    WHERE event_count BETWEEN 2 AND 6
    GROUP BY channel, DATE_TRUNC('MONTH', session_start)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE n_channel_not_in_web_email > (SELECT AVG(n_channel_not_in_web_email) FROM totals)
)
SELECT
    channel,
    session_start_month,
    first_session_end,
    burst_id_channel_not_in_email_chat,
    pct_channel_in_phone_chat_email,
    median_burst_id,
    event_count_to_burst_id_ratio,
    n_channel_not_in_web_email,
    DATEDIFF(DATE '2025-12-31', session_start_month) AS days_since_session_start_month,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
FROM with_activity
ORDER BY median_burst_id

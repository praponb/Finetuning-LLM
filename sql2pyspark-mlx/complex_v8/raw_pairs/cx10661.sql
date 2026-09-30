WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        LOWER(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE csat <= 5
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        channel,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, burst_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        channel,
        burst_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY events, YEAR(session_start) ORDER BY session_requester_id NULLS LAST), 3) AS session_requester_id_percent_rank,
        COUNT(*) OVER (PARTITION BY channel, TO_DATE(session_start)) AS rows_in_channel_session_start_to_date
    FROM session_summary
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(burst_id) OVER w_burst_id_channel AS burst_id_channel_min_burst_id,
        ROUND(AVG(burst_id) OVER (ORDER BY rows_in_channel_session_start_to_date, channel, burst_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS burst_id_avg_1_prior,
        COUNT(*) OVER (PARTITION BY burst_id, channel) AS rows_in_burst_id_channel,
        ROW_NUMBER() OVER w_all_by_rows_in_channel_sessi_2 AS seq_no
    FROM ranked
    WINDOW w_burst_id_channel AS (PARTITION BY burst_id, channel), w_all_by_rows_in_channel_sessi_2 AS (ORDER BY rows_in_channel_session_start_to_date, channel, burst_id)
)
SELECT channel, burst_id, metric_name, amount
FROM (
    SELECT channel, burst_id, burst_id_channel_min_burst_id, rows_in_burst_id_channel, rows_in_channel_session_start_to_date
    FROM windowed
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (burst_id_channel_min_burst_id, rows_in_burst_id_channel, rows_in_channel_session_start_to_date)
) unp
ORDER BY channel, burst_id, metric_name

WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-17 21:00:00'
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(priority)) AS priority_len,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label,
        UPPER(TRIM(priority)) AS priority_clean
    FROM valid
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'customer'
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(priority_len, 1, 0) OVER (ORDER BY priority_len DESC NULLS FIRST, id) AS prev_priority_len,
        MAX(priority_len) OVER (PARTITION BY priority, agent_id) AS priority_agent_id_max_priority_len,
        MAX(priority_len) OVER (ORDER BY priority_len DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS priority_len_max_2_prior
    FROM with_activity
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM metrics s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(priority_len / NULLIF(SUM(priority_len) OVER w_agent_id, 0), 4) AS priority_len_share_of_agent_id,
        ROUND(prev_priority_len - AVG(prev_priority_len) OVER (PARTITION BY agent_id), 2) AS prev_priority_len_vs_agent_id_avg,
        SUM(prev_priority_len) OVER (PARTITION BY agent_id) AS agent_id_sum_prev_priority_len
    FROM summarized_rows
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
with_activity_2 AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat < 4
              AND t.priority = 'P4'
        )
)
SELECT
    id,
    agent_id,
    priority,
    channel,
    priority_len,
    channel_priority_label,
    priority_clean,
    priority_agent_id_max_priority_len,
    priority_len_max_2_prior,
    last_event_at,
    priority_len_share_of_agent_id,
    prev_priority_len_vs_agent_id_avg,
    agent_id_sum_prev_priority_len,
    priority_len + prev_priority_len AS priority_len_plus_prev_priority_len,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
FROM with_activity_2
ORDER BY channel_or_unknown DESC, priority DESC

WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE channel NOT IN ('phone', 'chat')
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count
    FROM recent_tickets s
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(requester_id - 13) AS requester_id_distance,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at
    FROM with_counts
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS event_count_2,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_count_2, 0) AS event_count_2,
        t.first_event_at_2
    FROM derived s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        ROUND(requester_id_distance / NULLIF(SUM(requester_id_distance) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_distance_share_of_agent_id,
        ROUND(hours_opened_at_to_closed_at - AVG(hours_opened_at_to_closed_at) OVER (PARTITION BY agent_id), 2) AS hours_opened_at_to_closed_at_vs_agent_id_avg,
        ROUND(AVG(days_since_closed_at) OVER (PARTITION BY agent_id), 2) AS agent_id_avg_days_since_closed_at,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY event_count_2 NULLS LAST, id))) / 60, 1) AS minutes_since_prev_closed_at
    FROM summarized_rows
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    csat,
    tags,
    requester_id_per_csat,
    channel_or_unknown,
    hours_opened_at_to_closed_at,
    event_count,
    days_since_closed_at,
    event_count_2,
    first_event_at_2,
    rows_in_agent_id,
    requester_id_distance_share_of_agent_id,
    agent_id_avg_days_since_closed_at,
    minutes_since_prev_closed_at,
    SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
FROM with_windows

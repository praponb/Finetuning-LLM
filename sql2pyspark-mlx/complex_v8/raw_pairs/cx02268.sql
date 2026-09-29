WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ABS(csat - 3) AS csat_distance,
        closed_at + INTERVAL 15 MINUTES AS closed_at_plus_15m,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
    WHERE agent_id IS NOT NULL
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.csat_distance,
        s.closed_at_plus_15m,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at
    FROM line_level s
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_ticket_opened_at) AS first_ticket_opened_at_month,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        MAX(closed_at_plus_15m) AS last_closed_at_plus_15m,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        MAX(requester_id) AS max_requester_id
    FROM with_counts
    GROUP BY 1
)
-- Final projection
SELECT
    first_ticket_opened_at_month,
    priority_list,
    last_closed_at_plus_15m,
    channel_list,
    distinct_ids,
    max_requester_id,
    last_closed_at_plus_15m + INTERVAL 2 HOURS AS last_closed_at_plus_15m_plus_2h,
    YEAR(last_closed_at_plus_15m) AS last_closed_at_plus_15m_year
FROM rollup_base
ORDER BY last_closed_at_plus_15m DESC NULLS FIRST, last_closed_at_plus_15m_plus_2h

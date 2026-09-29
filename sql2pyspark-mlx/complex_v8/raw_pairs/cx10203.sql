WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref
    FROM tickets
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM recent_tickets s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        agent_id,
        csat,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        MIN(requester_id) AS min_requester_id,
        COUNT(*) FILTER (WHERE channel NOT IN ('email', 'web')) AS n_channel_not_in_email_web,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids
    FROM with_stats
    GROUP BY 1, 2, 3
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.opened_at_month,
        s.agent_id,
        s.csat,
        s.priority_list,
        s.channel_list,
        s.min_requester_id,
        s.distinct_agent_ids,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.team AS agent_team
    FROM rollup_base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM with_details_agent s
)
SELECT opened_at_month, agent_id, field, reading
FROM (
    SELECT opened_at_month, agent_id, distinct_agent_ids, csat
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (distinct_agent_ids, csat)
) unp
ORDER BY opened_at_month, agent_id, field

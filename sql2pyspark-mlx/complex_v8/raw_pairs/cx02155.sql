WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor NOT IN ('system', 'customer')
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.opened_at AS ticket_opened_at,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        su.agent_name
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE NOT ARRAY_CONTAINS(t.tags, 'upgrade')
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        HOUR(event_at) AS event_at_hour,
        SIZE(ticket_tags) AS ticket_tags_count
    FROM enriched_ticket
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM derived s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_at >= TIMESTAMP '2025-01-25 13:00:00'
              AND t.event_type IS NOT NULL
        )
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.days_since_event_at,
        s.ticket_tags,
        s.agent_name,
        s.event_at_hour,
        s.ticket_tags_count,
        t.csat AS ticket_csat,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        days_since_event_at,
        agent_name,
        event_at_hour,
        ticket_csat,
        agent_hired_on,
        agent_team,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY event_at_hour, event_type), 0), 4) AS days_since_event_at_share_of_event_at_hour_event,
        COUNT(*) OVER (PARTITION BY agent_team ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY agent_team ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        NTH_VALUE(ticket_tags_count, 2) OVER (PARTITION BY agent_team ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_tags_count,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY event_at_hour, event_type)) AS distinct_agent_team_in_event_at_hour_event_type
    FROM with_details
)
SELECT id, metric, metric_value
FROM (
    SELECT id, ticket_csat, distinct_agent_team_in_event_at_hour_event_type, days_since_event_at
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (ticket_csat AS m_ticket_csat, distinct_agent_team_in_event_at_hour_event_type AS m_distinct_agent_team_in_event_at_hour_event_typ, days_since_event_at AS days_since_event_at_v)
) unp
ORDER BY id, metric

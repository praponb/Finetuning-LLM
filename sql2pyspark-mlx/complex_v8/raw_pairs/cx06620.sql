WITH scoped_tickets AS (
    -- Filter tickets
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
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 9 AND 12
      AND NOT ARRAY_CONTAINS(tags, 'upgrade')
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        su.agent_name
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.agent_name,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM enriched_agent s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at >= TIMESTAMP '2025-02-06 09:00:00'
),
enriched_agent_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.agent_name,
        s.event_id,
        s.event_at,
        s.event_actor,
        s.event_type,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM line_level s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'Onboarding'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM enriched_agent_2 s
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
),
agent_id_stats AS (
    -- Median event_count per agent_id
    SELECT agent_id, PERCENTILE_APPROX(event_count, 0.5) AS median_event_count
    FROM screened
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_count,
        ROUND(s.event_count - g.median_event_count, 2) AS event_count_minus_median
    FROM screened s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.event_count >= g.median_event_count
)
SELECT id, event_id, field, metric_value
FROM (
    SELECT id, event_id, median_event_count, event_count_minus_median
    FROM outliers
) src
UNPIVOT (
    metric_value FOR field IN (median_event_count, event_count_minus_median)
) unp

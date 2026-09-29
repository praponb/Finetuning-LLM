WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('replied', 'created', 'resolved')
      AND event_at < TIMESTAMP '2025-02-06 12:00:00'
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.csat AS ticket_csat
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.ticket_csat,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id
),
derived AS (
    -- Add calculated columns
    SELECT *, ARRAY_CONTAINS(ticket_tags, 'login') AS has_login
    FROM with_details_ticket
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_csat,
        ticket_agent_id,
        ticket_channel,
        ticket_tags,
        has_login,
        COUNT(*) OVER (PARTITION BY ticket_agent_id) AS rows_in_ticket_agent_id,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, ticket_csat ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        LAG(event_at) OVER (PARTITION BY actor, ticket_csat ORDER BY event_at DESC, id) AS prev_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER w_event_at_year_ticket_id_by_t, 3) AS ticket_csat_percent_rank,
        FIRST_VALUE(ticket_csat) OVER (PARTITION BY actor, ticket_csat ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_csat
    FROM derived
    WINDOW w_event_at_year_ticket_id_by_t AS (PARTITION BY YEAR(event_at), ticket_id ORDER BY ticket_csat DESC)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    ticket_agent_id,
    ticket_channel,
    ticket_tags,
    has_login,
    rows_in_ticket_agent_id,
    event_at_cume_dist,
    prev_event_at,
    second_event_at,
    ticket_csat_percent_rank,
    first_ticket_csat,
    CASE WHEN has_login THEN 1 ELSE 0 END AS has_login_flag
FROM with_trends
WHERE rows_in_ticket_agent_id < 13
ORDER BY id NULLS LAST, prev_event_at

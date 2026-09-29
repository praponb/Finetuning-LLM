WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.channel = 'web'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(ticket_opened_at) OVER (PARTITION BY event_type ORDER BY ticket_opened_at DESC, id) AS next_ticket_opened_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, MONTH(ticket_opened_at) ORDER BY agent_hired_on, id), 3) AS agent_hired_on_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        NTILE(5) OVER (ORDER BY agent_hired_on DESC, id) AS agent_hired_on_quintile,
        ROW_NUMBER() OVER w_event_type_by_ticket_opened AS rn,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY ticket_opened_at DESC, id) AS prev_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM with_details_ticket
    WINDOW w_event_type_by_ticket_opened AS (PARTITION BY event_type ORDER BY ticket_opened_at DESC, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_trends
    WHERE rn <= 5
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_channel,
        ticket_opened_at,
        agent_name,
        agent_hired_on,
        first_known_event_at,
        next_ticket_opened_at,
        agent_hired_on_cume_dist,
        event_at_cume_dist,
        agent_hired_on_quintile,
        second_event_at,
        COUNT(*) OVER (PARTITION BY TO_DATE(event_at), ticket_id) AS rows_in_event_at_to_date_ticket_id,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_opened_at DESC NULLS FIRST, id), 3) AS ticket_opened_at_cume_dist,
        MAX(event_at_cume_dist) OVER (PARTITION BY TO_DATE(event_at), ticket_id) AS event_at_to_date_ticket_id_max_event_at_cume_dis
    FROM top_rows
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    ticket_channel,
    ticket_opened_at,
    agent_name,
    agent_hired_on,
    first_known_event_at,
    next_ticket_opened_at,
    agent_hired_on_cume_dist,
    event_at_cume_dist,
    second_event_at,
    ticket_opened_at_cume_dist,
    TO_DATE(next_ticket_opened_at) AS next_ticket_opened_at_date,
    LOWER(TRIM(actor)) AS actor_clean
FROM sequenced

WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label,
        TO_DATE(event_at) AS event_at_date,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE event_type = 'created'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        actor,
        event_type_actor_label,
        event_at_date,
        event_at_quarter,
        ROUND(AVG(event_at_quarter) OVER (PARTITION BY ticket_id ORDER BY event_at_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS event_at_quarter_avg_6_prior,
        DENSE_RANK() OVER w_ticket_id_by_event_at_date AS event_at_date_dense_rank
    FROM base_events
    WINDOW w_ticket_id_by_event_at_date AS (PARTITION BY ticket_id ORDER BY event_at_date DESC, id)
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags,
        su.team AS agent_team
    FROM windowed s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM with_parent_ticket s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.csat <= 3
        )
),
actor_stats AS (
    -- Max event_at_quarter per actor
    SELECT actor, MAX(event_at_quarter) AS max_event_at_quarter
    FROM without_matches
    GROUP BY actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_at_quarter,
        ROUND(s.event_at_quarter - g.max_event_at_quarter, 2) AS event_at_quarter_minus_max
    FROM without_matches s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_quarter = g.max_event_at_quarter
),
without_matches_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM outliers
    WHERE ticket_requester_id > (SELECT AVG(ticket_requester_id) FROM outliers)
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.channel AS ticket_channel
    FROM without_matches_2 s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel IN ('chat', 'web', 'email')
)
SELECT DISTINCT agent_team, id, ticket_tags
FROM combined_details_ticket
ORDER BY agent_team DESC

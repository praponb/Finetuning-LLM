WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE actor <> 'customer'
      AND event_type <> 'assigned'
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at,
        t.priority AS ticket_priority,
        su.tier AS agent_tier,
        su.agent_name
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ticket_priority, actor) AS rows_in_ticket_priority_actor,
        MAX(ticket_requester_id) OVER w_ticket_priority_actor AS ticket_priority_actor_max_ticket_requester_id,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY ticket_priority, actor)) AS distinct_ticket_id_in_ticket_priority_actor,
        ROUND(PERCENT_RANK() OVER w_ticket_priority_by_ticket_op, 3) AS ticket_opened_at_percent_rank,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_priority ORDER BY ticket_opened_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ticket_requester_id_avg_7,
        RANK() OVER w_ticket_priority_by_ticket_op AS ticket_opened_at_rank,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY ticket_priority, actor), 0), 4) AS ticket_requester_id_share_of_ticket_priority_act,
        LEAD(ticket_opened_at) OVER (PARTITION BY agent_tier ORDER BY ticket_requester_id DESC, id) AS next_ticket_opened_at
    FROM enriched
    WINDOW w_ticket_priority_actor AS (PARTITION BY ticket_priority, actor), w_ticket_priority_by_ticket_op AS (PARTITION BY ticket_priority ORDER BY ticket_opened_at DESC)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(ticket_opened_at_rank, 0) AS ticket_opened_at_rank_filled,
        ABS(ticket_requester_id - 16) AS ticket_requester_id_distance,
        DATE_TRUNC('HOUR', next_ticket_opened_at) AS next_ticket_opened_at_hour
    FROM with_windows
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    event_type_slug,
    ticket_requester_id,
    ticket_opened_at,
    ticket_priority,
    agent_tier,
    rows_in_ticket_priority_actor,
    distinct_ticket_id_in_ticket_priority_actor,
    ticket_opened_at_percent_rank,
    ticket_requester_id_avg_7,
    ticket_opened_at_rank,
    ticket_requester_id_share_of_ticket_priority_act,
    next_ticket_opened_at,
    ticket_opened_at_rank_filled,
    next_ticket_opened_at_hour,
    CONCAT(event_type_slug, '#', CAST(ticket_opened_at_rank AS STRING)) AS event_type_slug_ref,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM labeled
WHERE event_at < TIMESTAMP '2025-03-02 15:00:00'

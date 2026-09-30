WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE (event_at >= TIMESTAMP '2025-01-25 13:00:00' OR actor IN ('system', 'agent'))
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.requester_id AS ticket_requester_id,
        su.tier AS agent_tier,
        su.agent_name
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        agent_name,
        ROW_NUMBER() OVER w_agent_tier_by_event_at AS event_at_seq,
        LEAD(ticket_requester_id) OVER w_agent_tier_by_event_at AS next_ticket_requester_id,
        MAX(ticket_requester_id) OVER (ORDER BY event_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_4,
        FIRST_VALUE(ticket_requester_id) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_requester_id,
        MAX(ticket_requester_id) OVER (PARTITION BY agent_tier ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_requester_id_so_far,
        SIZE(COLLECT_SET(ticket_id) OVER w_event_type) AS distinct_ticket_id_in_event_type,
        MAX(ticket_requester_id) OVER w_ticket_id_event_type AS ticket_id_event_type_max_ticket_requester_id,
        DENSE_RANK() OVER (ORDER BY event_at, id) AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY ticket_id, event_type), 0), 4) AS ticket_requester_id_share_of_ticket_id_event_typ
    FROM with_parent
    WINDOW w_agent_tier_by_event_at AS (PARTITION BY agent_tier ORDER BY event_at DESC, id), w_event_type AS (PARTITION BY event_type), w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_at_seq, distinct_ticket_id_in_event_type ORDER BY event_at DESC), 3) AS event_at_percent_rank_2,
        LAG(ticket_requester_id_max_4) OVER w_all_by_ticket_requester_id_s AS prev_ticket_requester_id_max_4,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY event_at_dense_rank, distinct_ticket_id_in_event_type)) AS distinct_event_type_in_event_at_dense_rank_disti,
        LAG(event_at, 2) OVER (ORDER BY ticket_requester_id_share_of_ticket_id_event_typ DESC NULLS FIRST, id) AS prev2_event_at,
        MAX(event_at_percent_rank) OVER (ORDER BY next_ticket_requester_id DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_at_percent_rank_max_6_prior,
        ROUND(ticket_requester_id_max_4 / NULLIF(SUM(ticket_requester_id_max_4) OVER (PARTITION BY event_at_dense_rank, distinct_ticket_id_in_event_type), 0), 4) AS ticket_requester_id_max_4_share_of_event_at_dens
    FROM ranked
    WINDOW w_all_by_ticket_requester_id_s AS (ORDER BY ticket_requester_id_share_of_ticket_id_event_typ DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    event_type,
    agent_name,
    event_at_seq,
    next_ticket_requester_id,
    first_known_ticket_requester_id,
    max_ticket_requester_id_so_far,
    distinct_ticket_id_in_event_type,
    event_at_dense_rank,
    event_at_percent_rank,
    ticket_requester_id_share_of_ticket_id_event_typ,
    event_at_percent_rank_2,
    prev_ticket_requester_id_max_4,
    ticket_requester_id_max_4_share_of_event_at_dens
FROM windowed
ORDER BY ticket_requester_id_share_of_ticket_id_event_typ NULLS LAST

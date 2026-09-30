WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority,
        su.agent_name,
        su.tier AS agent_tier
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id AND TO_DATE(t.opened_at) < DATE '2025-02-24'
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        ticket_closed_at,
        ticket_priority,
        agent_tier,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        RANK() OVER (PARTITION BY ticket_priority ORDER BY ticket_closed_at DESC NULLS FIRST) AS ticket_closed_at_rank,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        LEAD(ticket_closed_at, 3) OVER (ORDER BY ticket_closed_at ASC NULLS LAST, id) AS next3_ticket_closed_at,
        ROW_NUMBER() OVER w_ticket_id_by_event_at AS event_at_seq,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY ticket_closed_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LEAD(ticket_closed_at, 2) OVER (PARTITION BY ticket_priority ORDER BY event_at, id) AS next2_ticket_closed_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY ticket_priority ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        DENSE_RANK() OVER w_ticket_id_by_event_at_ties AS event_at_dense_rank
    FROM with_parent
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC, id), w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at DESC)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        ticket_closed_at,
        agent_tier,
        first_known_event_at,
        first_known_ticket_closed_at,
        next3_ticket_closed_at,
        event_at_seq,
        last_known_event_at,
        next2_ticket_closed_at,
        second_ticket_closed_at,
        CASE WHEN event_at_dense_rank >= 9 THEN 'large' WHEN event_at_dense_rank >= 4 THEN 'medium' ELSE 'small' END AS event_at_dense_rank_tier
    FROM sequenced
    WHERE event_type IN ('reopened', 'replied', 'assigned')
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(second_ticket_closed_at) OVER (PARTITION BY YEAR(event_at) ORDER BY last_known_event_at, id) AS next_second_ticket_closed_at,
        COUNT(*) OVER (PARTITION BY YEAR(event_at) ORDER BY last_known_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (PARTITION BY YEAR(event_at) ORDER BY first_known_ticket_closed_at ASC, id) AS first_known_ticket_closed_at_seq,
        SIZE(COLLECT_SET(event_at_dense_rank_tier) OVER (PARTITION BY DATE_TRUNC('MONTH', next3_ticket_closed_at), MONTH(next2_ticket_closed_at))) AS distinct_event_at_dense_rank_tier_in_next3_ticke,
        ROUND(event_at_seq / NULLIF(SUM(event_at_seq) OVER (PARTITION BY DATE_TRUNC('MONTH', next3_ticket_closed_at), MONTH(next2_ticket_closed_at)), 0), 4) AS event_at_seq_share_of_next3_ticket_closed_at_mon,
        FIRST_VALUE(event_at_seq) IGNORE NULLS OVER (ORDER BY last_known_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_seq,
        RANK() OVER (PARTITION BY YEAR(event_at) ORDER BY last_known_event_at, id) AS last_known_event_at_rank,
        SUM(event_at_seq) OVER (ORDER BY last_known_event_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_at_seq_sum_6_prior
    FROM labeled
)
SELECT
    id,
    event_at,
    event_type,
    ticket_closed_at,
    agent_tier,
    first_known_ticket_closed_at,
    next3_ticket_closed_at,
    event_at_seq,
    last_known_event_at,
    next_second_ticket_closed_at,
    first_known_ticket_closed_at_seq,
    distinct_event_at_dense_rank_tier_in_next3_ticke,
    event_at_seq_share_of_next3_ticket_closed_at_mon,
    first_known_event_at_seq,
    last_known_event_at_rank,
    event_at_seq_sum_6_prior,
    LENGTH(TRIM(event_type)) AS event_type_len,
    CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
FROM sequenced_2
ORDER BY event_type_ref NULLS LAST

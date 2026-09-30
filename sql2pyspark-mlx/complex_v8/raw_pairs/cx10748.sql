WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 3)
      AND ticket_id >= 17
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_slug,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_slug,
        ticket_opened_at,
        ticket_agent_id,
        LEAD(ticket_opened_at) OVER w_ticket_id_by_event_at AS next_ticket_opened_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY ticket_opened_at ASC, id), 3) AS ticket_opened_at_cume_dist,
        LAST_VALUE(ticket_opened_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY ticket_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_opened_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_opened_at), 3) AS ticket_opened_at_percent_rank,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY event_type ORDER BY ticket_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(ticket_opened_at, 3) OVER w_all_by_ticket_opened_at AS prev3_ticket_opened_at,
        DENSE_RANK() OVER w_event_type_by_ticket_opened_4 AS ticket_opened_at_dense_rank
    FROM with_parent
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id), w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at, id), w_event_type_by_ticket_opened_4 AS (PARTITION BY event_type ORDER BY ticket_opened_at ASC)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        event_type,
        ticket_agent_id,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        MAX(last_event_at) AS last_last_event_at,
        MIN(prev3_ticket_opened_at) AS first_prev3_ticket_opened_at,
        COUNT(DISTINCT ticket_agent_id) AS distinct_ticket_agent_ids,
        ROUND(SUM(event_at_percent_rank), 2) AS total_event_at_percent_rank,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM ranked
    GROUP BY actor, event_type, ticket_agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(distinct_ticket_agent_ids) OVER (PARTITION BY event_type) AS event_type_sum_distinct_ticket_agent_ids,
        DENSE_RANK() OVER (PARTITION BY ticket_agent_id ORDER BY total_event_at_percent_rank ASC) AS total_event_at_percent_rank_dense_rank,
        ROUND(total_event_at_percent_rank / NULLIF(SUM(total_event_at_percent_rank) OVER (PARTITION BY event_type), 0), 4) AS total_event_at_percent_rank_share_of_event_type,
        ROUND(PERCENT_RANK() OVER w_ticket_agent_id_by_total_eve_2, 3) AS total_event_at_percent_rank_percent_rank
    FROM summary
    WINDOW w_ticket_agent_id_by_total_eve_2 AS (PARTITION BY ticket_agent_id ORDER BY total_event_at_percent_rank ASC, actor, event_type, ticket_agent_id)
)
-- Final projection
SELECT DISTINCT
    total_event_at_percent_rank_dense_rank,
    total_event_at_percent_rank_share_of_event_type,
    total_event_at_percent_rank_percent_rank
FROM with_windows
WHERE last_last_event_at >= TIMESTAMP '2025-01-25 14:00:00'

WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id <= 48
      AND event_type <> 'reopened'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_actor_label,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id AND t.opened_at < TIMESTAMP '2025-02-06 15:00:00'
    WHERE t.requester_id < 21
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_actor_label,
        ticket_requester_id,
        MAX(ticket_csat) OVER (PARTITION BY ticket_id, ticket_csat ORDER BY ticket_requester_id, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ticket_csat_max_6_prior,
        MIN(ticket_csat) OVER (PARTITION BY event_type, ticket_csat ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ticket_csat_so_far,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER w_actor, 0), 4) AS ticket_csat_share_of_actor,
        MAX(ticket_csat) OVER w_ticket_id AS ticket_id_max_ticket_csat,
        SUM(ticket_csat) OVER (PARTITION BY event_type, ticket_csat ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_csat,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_csat, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ticket_requester_id - LAG(ticket_requester_id) OVER (PARTITION BY ticket_csat ORDER BY ticket_requester_id DESC, id) AS ticket_requester_id_change,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_id), 2) AS ticket_id_avg_ticket_csat,
        SIZE(COLLECT_SET(event_type) OVER w_actor) AS distinct_event_type_in_actor,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_id, ticket_csat ORDER BY ticket_requester_id, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ticket_csat_avg_3_prior,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_csat ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTH_VALUE(ticket_csat, 2) OVER (PARTITION BY ticket_csat ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat,
        FIRST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY ticket_csat ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_requester_id,
        MIN(ticket_csat) OVER w_actor AS actor_min_ticket_csat,
        DENSE_RANK() OVER w_ticket_csat_event_type_by_ev_2 AS event_at_dense_rank
    FROM with_details
    WINDOW w_actor AS (PARTITION BY actor), w_ticket_id AS (PARTITION BY ticket_id), w_ticket_csat_event_type_by_ev_2 AS (PARTITION BY ticket_csat, event_type ORDER BY event_at)
),
per_group AS (
    -- Summarise per group
    SELECT
        distinct_event_type_in_actor,
        DATE_FORMAT(last_known_event_at, 'yyyy-MM') AS last_known_event_at_ym,
        MAX(first_known_event_at) AS last_first_known_event_at,
        COUNT(*) AS cnt
    FROM sequenced
    GROUP BY 1, 2
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_distinct_event_type_in_actor AS rows_in_distinct_event_type_in_actor,
        LAST_VALUE(last_first_known_event_at) OVER (PARTITION BY cnt, distinct_event_type_in_actor ORDER BY last_first_known_event_at ASC, distinct_event_type_in_actor, last_known_event_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_first_known_event_at
    FROM per_group
    WINDOW w_distinct_event_type_in_actor AS (PARTITION BY distinct_event_type_in_actor)
)
SELECT
    distinct_event_type_in_actor,
    last_known_event_at_ym,
    last_first_known_event_at,
    cnt,
    last_last_first_known_event_at
FROM sequenced_2
ORDER BY last_first_known_event_at

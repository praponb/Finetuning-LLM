WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_prefix,
        s.event_at_plus_1d,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ticket_agent_id) AS rows_in_ticket_agent_id,
        ROW_NUMBER() OVER w_ticket_id_actor_by_event_at AS event_at_seq,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY ticket_requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        MAX(ticket_requester_id) OVER (ORDER BY ticket_requester_id DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_4,
        DENSE_RANK() OVER w_ticket_id_actor_by_event_at_2 AS event_at_dense_rank,
        FIRST_VALUE(event_at_plus_1d) IGNORE NULLS OVER (ORDER BY ticket_requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_plus_1d,
        SUM(ticket_requester_id) OVER w_event_at_to_date AS event_at_to_date_sum_ticket_requester_id,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_requester_id DESC NULLS FIRST, id), 3) AS ticket_requester_id_cume_dist,
        ticket_requester_id - LAG(ticket_requester_id) OVER w_ticket_id_actor_by_event_at AS ticket_requester_id_change,
        ROUND((UNIX_TIMESTAMP(event_at_plus_1d) - UNIX_TIMESTAMP(LAG(event_at_plus_1d) OVER (ORDER BY ticket_requester_id NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at_plus_1d,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER w_event_at_to_date, 2) AS ticket_requester_id_vs_event_at_to_date_avg,
        LAST_VALUE(ticket_requester_id) OVER (ORDER BY ticket_requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_requester_id
    FROM combined_details_ticket
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id), w_ticket_id_actor_by_event_at_2 AS (PARTITION BY ticket_id, actor ORDER BY event_at ASC), w_event_at_to_date AS (PARTITION BY TO_DATE(event_at))
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE event_at_dense_rank <= 2
      AND ticket_requester_id BETWEEN 2 AND 24
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_plus_1d,
        ticket_requester_id,
        rows_in_ticket_agent_id,
        first_known_event_at,
        ticket_requester_id_max_4,
        event_at_dense_rank,
        first_known_event_at_plus_1d,
        ticket_requester_id_cume_dist,
        minutes_since_prev_event_at_plus_1d,
        ticket_requester_id_vs_event_at_to_date_avg,
        last_ticket_requester_id,
        SUM(event_at_seq) OVER (ORDER BY ticket_requester_id_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_seq,
        COUNT(*) OVER (PARTITION BY ticket_id) AS rows_in_ticket_id,
        ROW_NUMBER() OVER (PARTITION BY event_at_dense_rank, ticket_agent_id ORDER BY ticket_requester_id_change DESC, id) AS ticket_requester_id_change_seq,
        SUM(ticket_requester_id_change) OVER (ORDER BY ticket_requester_id_change DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_change_sum_2_prior,
        MAX(ticket_requester_id_max_4) OVER (PARTITION BY ticket_id) AS ticket_id_max_ticket_requester_id_max_4,
        LAST_VALUE(event_at_dense_rank) OVER (ORDER BY ticket_requester_id_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_dense_rank,
        NTILE(10) OVER w_all_by_ticket_requester_id_c_4 AS ticket_requester_id_change_decile
    FROM top_per_group
    WINDOW w_all_by_ticket_requester_id_c_4 AS (ORDER BY ticket_requester_id_change DESC, id)
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    event_at_plus_1d,
    ticket_requester_id,
    rows_in_ticket_agent_id,
    first_known_event_at,
    ticket_requester_id_max_4,
    minutes_since_prev_event_at_plus_1d,
    ticket_requester_id_vs_event_at_to_date_avg,
    rows_in_ticket_id,
    ticket_id_max_ticket_requester_id_max_4,
    last_event_at_dense_rank,
    ticket_requester_id_change_decile,
    CONCAT(event_type, '#', CAST(running_event_at_seq AS STRING)) AS event_type_ref
FROM with_trends_2
ORDER BY ticket_requester_id_change_decile, ticket_id_max_ticket_requester_id_max_4 DESC NULLS FIRST

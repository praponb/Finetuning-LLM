WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.priority NOT IN ('P2', 'P3')
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(ticket_requester_id) OVER w_event_type_actor AS event_type_actor_sum_ticket_requester_id,
        MIN(ticket_requester_id) OVER (ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ticket_requester_id_so_far,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY actor ORDER BY ticket_requester_id, id)) AS days_since_prev_agent_hired_on,
        COUNT(*) OVER (PARTITION BY actor, event_type) AS rows_in_actor_event_type,
        FIRST_VALUE(ticket_requester_id) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_requester_id,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY ticket_requester_id, id) AS ticket_requester_id_dense_rank,
        MAX(ticket_requester_id) OVER (PARTITION BY actor ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_requester_id_so_far,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY event_type, actor), 2) AS ticket_requester_id_vs_event_type_actor_avg,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY ticket_requester_id, id) AS seq_no,
        MAX(ticket_requester_id) OVER (PARTITION BY actor, event_type) AS actor_event_type_max_ticket_requester_id,
        MAX(ticket_requester_id) OVER (PARTITION BY actor, event_type ORDER BY ticket_closed_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_max_1_prior,
        ROUND(AVG(ticket_requester_id) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ticket_requester_id_avg_3,
        ROW_NUMBER() OVER (ORDER BY ticket_closed_at DESC, id) AS seq_no_2
    FROM with_parent
    WINDOW w_event_type_actor AS (PARTITION BY event_type, actor)
),
annotated AS (
    -- Derive helper columns
    SELECT *, CONCAT(agent_name, '#', CAST(seq_no AS STRING)) AS agent_name_ref
    FROM metrics
    WHERE ticket_requester_id_max_1_prior <= 53
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(ticket_requester_id_avg_3) OVER (PARTITION BY event_type) AS event_type_sum_ticket_requester_id_avg_3,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type,
        NTILE(10) OVER w_ticket_id_seq_no_2_by_ticket AS ticket_closed_at_decile,
        MAX(seq_no) OVER (PARTITION BY ticket_id, TO_DATE(ticket_closed_at)) AS ticket_id_ticket_closed_at_to_date_max_seq_no,
        NTH_VALUE(seq_no_2, 2) OVER (ORDER BY ticket_requester_id_max_1_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_seq_no_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_requester_id_max_1_prior DESC), 3) AS ticket_requester_id_max_1_prior_percent_rank
    FROM annotated
    WINDOW w_ticket_id_seq_no_2_by_ticket AS (PARTITION BY ticket_id, seq_no_2 ORDER BY ticket_closed_at, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    ticket_closed_at,
    ticket_requester_id,
    agent_hired_on,
    agent_name,
    event_type_actor_sum_ticket_requester_id,
    min_ticket_requester_id_so_far,
    days_since_prev_agent_hired_on,
    rows_in_actor_event_type,
    first_ticket_requester_id,
    max_ticket_requester_id_so_far,
    ticket_requester_id_vs_event_type_actor_avg,
    seq_no,
    actor_event_type_max_ticket_requester_id,
    ticket_requester_id_avg_3,
    seq_no_2,
    agent_name_ref,
    ticket_closed_at_decile,
    ticket_id_ticket_closed_at_to_date_max_seq_no,
    second_seq_no_2,
    ticket_requester_id_max_1_prior_percent_rank,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
FROM with_trends
WHERE agent_name_ref IS NOT NULL
ORDER BY agent_name DESC, actor DESC NULLS FIRST

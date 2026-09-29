WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE ticket_id <= 10
      AND event_at < TIMESTAMP '2025-03-08 09:00:00'
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        t.priority AS ticket_priority
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor_clean,
        actor_ref,
        ticket_opened_at,
        ticket_agent_id,
        ticket_priority,
        NTILE(5) OVER w_event_type_by_event_at AS event_at_quintile,
        RANK() OVER w_ticket_agent_id_by_ticket_op AS ticket_opened_at_rank,
        LAG(ticket_opened_at, 3) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev3_ticket_opened_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_agent_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(ticket_opened_at) IGNORE NULLS OVER (PARTITION BY ticket_agent_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_opened_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS event_at_seq,
        NTILE(3) OVER (PARTITION BY ticket_agent_id ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_tercile,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_dense_rank
    FROM enriched_ticket
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, id), w_ticket_agent_id_by_ticket_op AS (PARTITION BY ticket_agent_id ORDER BY ticket_opened_at DESC)
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM sequenced
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        ticket_opened_at,
        ticket_agent_id,
        ticket_priority,
        event_at_quintile,
        ticket_opened_at_rank,
        prev3_ticket_opened_at,
        minutes_since_prev_event_at,
        last_known_ticket_opened_at,
        event_at_seq,
        ticket_opened_at_tercile,
        event_at_dense_rank,
        FIRST_VALUE(minutes_since_prev_event_at) OVER (PARTITION BY event_at_quintile ORDER BY ticket_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_minutes_since_prev_event_at,
        SUM(event_at_seq) OVER (ORDER BY second_event_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS event_at_seq_sum_2,
        ROUND(AVG(ticket_opened_at_tercile) OVER (ORDER BY second_event_at NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ticket_opened_at_tercile_avg_3_prior,
        LAG(ticket_opened_at_rank) OVER (ORDER BY second_event_at NULLS LAST, id) AS prev_ticket_opened_at_rank,
        LEAD(ticket_opened_at_tercile, 2) OVER w_all_by_second_event_at AS next2_ticket_opened_at_tercile,
        FIRST_VALUE(ticket_opened_at_tercile) OVER (ORDER BY event_at_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_opened_at_tercile,
        ROW_NUMBER() OVER (PARTITION BY event_at_quintile ORDER BY ticket_opened_at ASC, id) AS rn
    FROM tagged
    WINDOW w_all_by_second_event_at AS (ORDER BY second_event_at NULLS LAST, id)
)
SELECT
    id,
    event_type,
    ticket_agent_id,
    ticket_priority,
    event_at_quintile,
    ticket_opened_at_rank,
    prev3_ticket_opened_at,
    minutes_since_prev_event_at,
    last_known_ticket_opened_at,
    event_at_seq,
    ticket_opened_at_tercile,
    event_at_dense_rank,
    first_minutes_since_prev_event_at,
    event_at_seq_sum_2,
    ticket_opened_at_tercile_avg_3_prior,
    prev_ticket_opened_at_rank,
    next2_ticket_opened_at_tercile,
    first_ticket_opened_at_tercile,
    rn,
    FLOOR(event_at_quintile / 2) * 2 AS event_at_quintile_bucket
FROM with_trends
ORDER BY event_at_dense_rank NULLS LAST, ticket_opened_at_tercile_avg_3_prior DESC, id
LIMIT 10

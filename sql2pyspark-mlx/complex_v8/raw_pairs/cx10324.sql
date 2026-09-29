WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-12'
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_prefix,
        t.tags AS ticket_tags
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE ARRAY_CONTAINS(t.tags, 'refund')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        event_type_prefix,
        ticket_tags,
        ROW_NUMBER() OVER w_actor_by_event_at AS event_at_seq,
        ROUND(PERCENT_RANK() OVER w_actor_by_event_at_ties, 3) AS event_at_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS event_at_rank,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS next_event_at,
        ROUND(PERCENT_RANK() OVER w_event_type_actor_by_event_at_2, 3) AS event_at_percent_rank_2,
        ROW_NUMBER() OVER w_event_type_actor_by_event_at AS seq_no,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM with_parent
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at, id), w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at), w_event_type_actor_by_event_at_2 AS (PARTITION BY event_type, actor ORDER BY event_at DESC), w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        event_at,
        event_type,
        event_type_prefix,
        ticket_tags,
        event_at_seq,
        event_at_percent_rank,
        second_event_at,
        event_at_rank,
        next_event_at,
        event_at_percent_rank_2,
        last_event_at
    FROM metrics
    WHERE seq_no = 1
      AND HOUR(last_event_at) BETWEEN 8 AND 13
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        ticket_tags,
        event_at_seq,
        second_event_at,
        next_event_at,
        event_at_percent_rank_2,
        last_event_at,
        LAST_VALUE(next_event_at) IGNORE NULLS OVER (ORDER BY last_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_event_at,
        LEAD(event_at_percent_rank, 3) OVER w_all_by_last_event_at AS next3_event_at_percent_rank,
        ROUND(AVG(event_at_percent_rank_2) OVER (PARTITION BY event_type), 2) AS event_type_avg_event_at_percent_rank_2,
        ROUND(AVG(event_at_seq) OVER (ORDER BY last_event_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS event_at_seq_avg_1_prior
    FROM top_per_group
    WINDOW w_all_by_last_event_at AS (ORDER BY last_event_at, id)
)
SELECT
    id,
    event_at,
    ticket_tags,
    event_at_seq,
    second_event_at,
    next_event_at,
    event_at_percent_rank_2,
    last_event_at,
    last_known_next_event_at,
    next3_event_at_percent_rank
FROM sequenced
ORDER BY next_event_at DESC, next3_event_at_percent_rank DESC NULLS FIRST

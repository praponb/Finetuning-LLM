WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_len,
        s.event_at_hour,
        t.csat AS ticket_csat
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY actor), 2) AS ticket_csat_vs_actor_avg,
        FIRST_VALUE(ticket_csat) IGNORE NULLS OVER (ORDER BY event_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_csat,
        LEAD(event_at, 2) OVER (PARTITION BY TO_DATE(event_at), ticket_csat ORDER BY actor_len DESC, id) AS next2_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at_hour DESC, id) AS seq_no,
        RANK() OVER (PARTITION BY actor ORDER BY actor_len DESC) AS actor_len_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY actor_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        actor_len - LAG(actor_len) OVER w_all_by_event_at_hour AS actor_len_change,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor)) AS distinct_ticket_id_in_actor
    FROM enriched
    WINDOW w_all_by_event_at_hour AS (ORDER BY event_at_hour DESC, id)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE seq_no <= 5
      AND distinct_ticket_id_in_actor <= 9
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor_len,
    event_at_hour,
    ticket_csat,
    ticket_csat_vs_actor_avg,
    first_known_ticket_csat,
    next2_event_at,
    seq_no,
    last_event_at_2,
    rows_in_actor,
    actor_len_change,
    distinct_ticket_id_in_actor,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    INITCAP(TRIM(event_type)) AS event_type_clean
FROM leaders
ORDER BY event_type_or_unknown

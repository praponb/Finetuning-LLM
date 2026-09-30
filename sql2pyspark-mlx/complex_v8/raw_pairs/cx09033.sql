WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE event_type <> 'created'
      AND event_at >= TIMESTAMP '2025-01-25 11:00:00'
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_csat,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_actor_ticket_csat_by_event_a)) / 60, 1) AS minutes_since_prev_event_at,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY ticket_closed_at ASC, id) AS ticket_closed_at_seq,
        MIN(ticket_csat) OVER (PARTITION BY ticket_csat, event_type) AS ticket_csat_event_type_min_ticket_csat,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_csat, event_type), 2) AS ticket_csat_event_type_avg_ticket_csat,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY ticket_closed_at ASC) AS ticket_closed_at_rank,
        RANK() OVER w_event_at_month_by_ticket_clo AS ticket_closed_at_rank_2,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY ticket_closed_at ASC, id) AS ticket_closed_at_quintile,
        COUNT(*) OVER (PARTITION BY ticket_csat, event_type) AS rows_in_ticket_csat_event_type,
        ticket_csat - LAG(ticket_csat) OVER w_event_at_month_by_ticket_clo AS ticket_csat_change,
        ROW_NUMBER() OVER (PARTITION BY actor, ticket_csat ORDER BY event_at DESC, id) AS event_at_seq,
        MAX(ticket_csat) OVER (PARTITION BY actor, ticket_csat ORDER BY event_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ticket_csat_max_6_prior
    FROM enriched_ticket
    WINDOW w_actor_ticket_csat_by_event_a AS (PARTITION BY actor, ticket_csat ORDER BY event_at DESC, id), w_event_at_month_by_ticket_clo AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY ticket_closed_at ASC, id), w_ticket_csat_event_type AS (PARTITION BY ticket_csat, event_type)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_csat,
        minutes_since_prev_event_at,
        ticket_closed_at_seq,
        ticket_csat_event_type_min_ticket_csat,
        ticket_csat_event_type_avg_ticket_csat,
        ticket_closed_at_rank,
        ticket_closed_at_quintile,
        rows_in_ticket_csat_event_type,
        ticket_csat_change,
        event_at_seq,
        ticket_csat_max_6_prior
    FROM with_trends
    WHERE ticket_closed_at_rank_2 <= 2
),
per_group AS (
    -- Summarise per group
    SELECT
        ticket_id,
        SUM(rows_in_ticket_csat_event_type) FILTER (WHERE event_type IN ('assigned', 'resolved')) AS rows_in_ticket_csat_event_type_event_type_in_ass,
        ROUND(AVG(ticket_csat_change), 2) AS avg_ticket_csat_change,
        COUNT(*) AS row_count,
        ROUND(STDDEV(ticket_csat_event_type_min_ticket_csat), 2) AS stddev_ticket_csat_event_type_min_ticket_csat,
        ROUND(SUM(ticket_closed_at_rank) / NULLIF(SUM(ticket_closed_at_seq), 0), 3) AS ticket_closed_at_rank_to_ticket_closed_at_seq_ra,
        SUM(CASE WHEN actor IS NOT NULL THEN 1 ELSE 0 END) AS n_actor_is_not_null
    FROM top_rows
    GROUP BY ticket_id
)
-- Final projection
SELECT
    ticket_id,
    rows_in_ticket_csat_event_type_event_type_in_ass,
    avg_ticket_csat_change,
    row_count,
    stddev_ticket_csat_event_type_min_ticket_csat,
    ticket_closed_at_rank_to_ticket_closed_at_seq_ra,
    n_actor_is_not_null
FROM per_group
ORDER BY row_count, ticket_id DESC

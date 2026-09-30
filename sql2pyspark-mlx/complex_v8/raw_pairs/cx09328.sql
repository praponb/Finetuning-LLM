WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor = 'agent'
      AND event_type IN ('resolved', 'reopened')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        days_since_event_at,
        SUM(days_since_event_at) OVER (PARTITION BY ticket_id, event_type) AS ticket_id_event_type_sum_days_since_event_at,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY actor), 0), 4) AS days_since_event_at_share_of_actor,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_id, actor ORDER BY days_since_event_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS days_since_event_at_max_3,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_ticket_id_event_type, 0), 4) AS days_since_event_at_share_of_ticket_id_event_typ,
        MAX(days_since_event_at) OVER (PARTITION BY actor) AS actor_max_days_since_event_at,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY ticket_id, event_type), 2) AS ticket_id_event_type_avg_days_since_event_at
    FROM cleaned
    WINDOW w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        days_since_event_at,
        ticket_id_event_type_sum_days_since_event_at,
        days_since_event_at_share_of_actor,
        rows_in_actor,
        days_since_event_at_share_of_ticket_id_event_typ,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_id, event_type) AS ticket_id_event_type_max_days_since_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_type ORDER BY days_since_event_at), 3) AS days_since_event_at_cume_dist,
        ROUND(rows_in_actor - AVG(rows_in_actor) OVER (PARTITION BY ticket_id, event_type), 2) AS rows_in_actor_vs_ticket_id_event_type_avg,
        COUNT(*) OVER (PARTITION BY ticket_id, event_type) AS rows_in_ticket_id_event_type,
        SUM(rows_in_actor) OVER (ORDER BY event_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rows_in_actor_sum_7,
        SUM(days_since_event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_event_at
    FROM with_trends
)
-- Final projection
SELECT
    id,
    ticket_id,
    days_since_event_at,
    ticket_id_event_type_sum_days_since_event_at,
    days_since_event_at_share_of_actor,
    rows_in_actor,
    days_since_event_at_share_of_ticket_id_event_typ,
    ticket_id_event_type_max_days_since_event_at,
    rows_in_actor_vs_ticket_id_event_type_avg,
    running_days_since_event_at,
    days_since_event_at + event_at_percent_rank AS days_since_event_at_plus_event_at_percent_rank
FROM windowed
ORDER BY ticket_id DESC

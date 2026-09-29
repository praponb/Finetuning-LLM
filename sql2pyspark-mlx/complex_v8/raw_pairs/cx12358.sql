WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 08:00:00'
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.opened_at AS ticket_opened_at,
        t.priority AS ticket_priority
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.priority IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LEAD(ticket_opened_at, 2) OVER w_all_by_event_at AS next2_ticket_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(ticket_opened_at) - UNIX_TIMESTAMP(LAG(ticket_opened_at) OVER (ORDER BY event_at ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_ticket_opened_at,
        ROUND(PERCENT_RANK() OVER w_event_at_to_date_by_event_at_2, 3) AS event_at_percent_rank_2
    FROM with_parent_ticket
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC NULLS LAST, id), w_event_at_to_date_by_event_at_2 AS (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id)
),
with_parent_ticket_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.ticket_opened_at,
        s.second_event_at,
        s.next2_ticket_opened_at,
        s.event_at_percent_rank,
        s.minutes_since_prev_ticket_opened_at,
        s.event_at_percent_rank_2,
        t.priority AS ticket_priority_2
    FROM windowed s
    JOIN tickets t ON t.id = s.ticket_id
),
grouped AS (
    -- Group-level totals
    SELECT
        event_type,
        ticket_priority_2,
        TO_DATE(event_at) AS event_at_day,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN ticket_priority_2 IN ('P1', 'P3', 'P4') THEN 1 ELSE 0 END) AS n_ticket_priority_2_in_p1_p3_p,
        COUNT(*) AS row_count
    FROM with_parent_ticket_2
    GROUP BY event_type, ticket_priority_2, TO_DATE(event_at)
)
-- Final projection
SELECT DISTINCT event_type, event_at_day
FROM grouped
ORDER BY event_type NULLS LAST

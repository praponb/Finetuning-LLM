WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 12:00:00'
      AND event_type IN ('reopened', 'escalated', 'resolved')
),
combined_details AS (
    -- Attach ticket attributes
    SELECT s.*, t.closed_at AS ticket_closed_at, t.opened_at AS ticket_opened_at
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        ticket_closed_at,
        ticket_opened_at,
        LAST_VALUE(ticket_opened_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY ticket_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_opened_at,
        NTILE(3) OVER w_actor_by_ticket_opened_at AS ticket_opened_at_tercile,
        LAG(event_at) OVER w_actor_by_ticket_opened_at AS prev_event_at,
        LEAD(event_at) OVER (PARTITION BY actor ORDER BY ticket_opened_at ASC, id) AS next_event_at
    FROM combined_details
    WINDOW w_actor_by_ticket_opened_at AS (PARTITION BY actor ORDER BY ticket_opened_at ASC, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        ticket_closed_at,
        last_known_ticket_opened_at,
        ticket_opened_at_tercile,
        prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(ticket_opened_at), DATE_TRUNC('MONTH', ticket_closed_at) ORDER BY last_known_ticket_opened_at NULLS LAST, id), 3) AS last_known_ticket_opened_at_cume_dist,
        ROUND(AVG(ticket_opened_at_tercile) OVER (PARTITION BY TO_DATE(ticket_opened_at), DATE_TRUNC('MONTH', ticket_closed_at) ORDER BY last_known_ticket_opened_at NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ticket_opened_at_tercile_avg_3,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_closed_at DESC), 3) AS ticket_closed_at_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_closed_at DESC), 3) AS ticket_closed_at_cume_dist,
        ROUND(ticket_opened_at_tercile / NULLIF(SUM(ticket_opened_at_tercile) OVER (PARTITION BY YEAR(ticket_closed_at), MONTH(next_event_at)), 0), 4) AS ticket_opened_at_tercile_share_of_ticket_closed_,
        ticket_opened_at_tercile - LAG(ticket_opened_at_tercile) OVER (ORDER BY ticket_closed_at DESC, id) AS ticket_opened_at_tercile_change
    FROM ranked
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_closed_at,
        last_known_ticket_opened_at,
        ticket_opened_at_tercile,
        prev_event_at,
        last_known_ticket_opened_at_cume_dist,
        ticket_opened_at_tercile_avg_3,
        ticket_closed_at_cume_dist,
        ticket_opened_at_tercile_share_of_ticket_closed_,
        LENGTH(TRIM(event_type)) AS event_type_len,
        ABS(ticket_opened_at_tercile - 5) AS ticket_opened_at_tercile_distance,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM windowed
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(prev_event_at) - UNIX_TIMESTAMP(LAG(prev_event_at) OVER w_all_by_ticket_closed_at_cume)) / 60, 1) AS minutes_since_prev_prev_event_at,
        NTILE(4) OVER w_all_by_ticket_closed_at_cume AS ticket_closed_at_cume_dist_quartile,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY ticket_opened_at_tercile_distance ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        COUNT(*) OVER w_ticket_closed_at_year_ticket AS rows_in_ticket_closed_at_year_ticket_opened_at_t
    FROM enriched
    WINDOW w_all_by_ticket_closed_at_cume AS (ORDER BY ticket_closed_at_cume_dist, id), w_ticket_closed_at_year_ticket AS (PARTITION BY YEAR(ticket_closed_at), ticket_opened_at_tercile)
)
SELECT id, field, amount
FROM (
    SELECT id, ticket_opened_at_tercile_distance, event_type_len, ticket_opened_at_tercile, ticket_closed_at_cume_dist_quartile
    FROM with_windows
) src
UNPIVOT (
    amount FOR field IN (ticket_opened_at_tercile_distance, event_type_len, ticket_opened_at_tercile, ticket_closed_at_cume_dist_quartile)
) unp
ORDER BY id, field

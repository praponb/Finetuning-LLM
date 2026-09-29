WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_ref,
        t.closed_at AS ticket_closed_at
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id) AS prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        NTILE(4) OVER (ORDER BY ticket_closed_at, id) AS ticket_closed_at_quartile,
        LAST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_3,
        NTILE(3) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id) AS event_at_tercile,
        RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id) AS event_at_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM enriched_ticket
    WINDOW w_event_at_month_by_event_at AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_last_event_at_month_by_event, 3) AS event_at_rank_percent_rank,
        FIRST_VALUE(last_event_at_3) IGNORE NULLS OVER (PARTITION BY event_at_tercile, event_at_rank ORDER BY ticket_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_event_at_3,
        ROUND(AVG(event_at_percent_rank) OVER w_ticket_closed_at_quartile, 2) AS ticket_closed_at_quartile_avg_event_at_percent_r,
        SUM(event_at_tercile) OVER (PARTITION BY ticket_closed_at_quartile) AS ticket_closed_at_quartile_sum_event_at_tercile,
        COUNT(*) OVER w_ticket_closed_at_quartile AS rows_in_ticket_closed_at_quartile,
        ROUND(event_at_percent_rank / NULLIF(SUM(event_at_percent_rank) OVER w_ticket_closed_at_quartile, 0), 4) AS event_at_percent_rank_share_of_ticket_closed_at_,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(last_event_at) ORDER BY event_at_rank DESC, id), 3) AS event_at_rank_cume_dist,
        MIN(event_at_rank) OVER w_ticket_closed_at_quartile AS ticket_closed_at_quartile_min_event_at_rank
    FROM ranked
    WINDOW w_last_event_at_month_by_event AS (PARTITION BY MONTH(last_event_at) ORDER BY event_at_rank DESC), w_ticket_closed_at_quartile AS (PARTITION BY ticket_closed_at_quartile)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor_ref,
    event_at_percent_rank,
    last_event_at,
    prev_event_at,
    minutes_since_prev_event_at,
    second_ticket_closed_at,
    minutes_since_prev_ticket_closed_at,
    ticket_closed_at_quartile,
    last_event_at_2,
    event_at_tercile,
    event_at_rank,
    ticket_closed_at_quartile_avg_event_at_percent_r,
    ticket_closed_at_quartile_sum_event_at_tercile,
    rows_in_ticket_closed_at_quartile,
    event_at_percent_rank_share_of_ticket_closed_at_,
    event_at_rank_cume_dist,
    COALESCE(event_at_percent_rank, 0.0) AS event_at_percent_rank_filled,
    ROUND(event_at_rank_cume_dist * 1.5, 2) AS event_at_rank_cume_dist_adj
FROM windowed
ORDER BY event_at_tercile DESC, event_at_rank_cume_dist_adj DESC, id
LIMIT 10

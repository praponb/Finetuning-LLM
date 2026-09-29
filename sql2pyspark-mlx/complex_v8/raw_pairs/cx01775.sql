WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        CASE WHEN event_type IN ('created', 'resolved') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
      AND ticket_id <= 9
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.actor_group,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY ticket_priority, ticket_channel ORDER BY ticket_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        NTILE(10) OVER w_ticket_channel_ticket_id_by AS ticket_closed_at_decile,
        NTH_VALUE(ticket_closed_at, 2) OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at_2,
        ROUND(PERCENT_RANK() OVER w_ticket_priority_ticket_chann_2, 3) AS ticket_closed_at_percent_rank,
        ROW_NUMBER() OVER (ORDER BY ticket_closed_at, id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_priority, ticket_channel ORDER BY ticket_closed_at ASC), 3) AS ticket_closed_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY ticket_channel, ticket_id ORDER BY ticket_closed_at, id) AS seq_no_2,
        LAG(ticket_closed_at) OVER (PARTITION BY ticket_channel, ticket_id ORDER BY ticket_closed_at, id) AS prev_ticket_closed_at
    FROM enriched_ticket
    WINDOW w_ticket_channel_ticket_id_by AS (PARTITION BY ticket_channel, ticket_id ORDER BY ticket_closed_at, id), w_ticket_priority_ticket_chann_2 AS (PARTITION BY ticket_priority, ticket_channel ORDER BY ticket_closed_at ASC)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE seq_no = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        actor_group,
        ticket_priority,
        second_ticket_closed_at,
        ticket_closed_at_decile,
        second_ticket_closed_at_2,
        ticket_closed_at_percent_rank,
        prev_ticket_closed_at,
        ROUND(AVG(ticket_closed_at_cume_dist) OVER (PARTITION BY ticket_closed_at_decile, ticket_channel), 2) AS ticket_closed_at_decile_ticket_channel_avg_ticke,
        NTH_VALUE(ticket_closed_at_decile, 2) OVER (ORDER BY prev_ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at_decile,
        ROUND((UNIX_TIMESTAMP(second_ticket_closed_at_2) - UNIX_TIMESTAMP(LAG(second_ticket_closed_at_2) OVER (PARTITION BY MONTH(second_ticket_closed_at), ticket_channel ORDER BY ticket_closed_at_cume_dist, id))) / 60, 1) AS minutes_since_prev_second_ticket_closed_at_2,
        LEAD(ticket_closed_at) OVER (ORDER BY second_ticket_closed_at_2, id) AS next_ticket_closed_at
    FROM leaders
)
SELECT
    id,
    actor_group,
    ticket_priority,
    second_ticket_closed_at,
    ticket_closed_at_decile,
    second_ticket_closed_at_2,
    prev_ticket_closed_at,
    ticket_closed_at_decile_ticket_channel_avg_ticke,
    second_ticket_closed_at_decile,
    next_ticket_closed_at,
    FLOOR(ticket_closed_at_decile / 2) * 2 AS ticket_closed_at_decile_bucket
FROM windowed
ORDER BY actor_group

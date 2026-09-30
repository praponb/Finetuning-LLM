WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 07:00:00'
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.opened_at >= TIMESTAMP '2025-02-06 12:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        ticket_requester_id,
        ticket_priority,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, ticket_priority ORDER BY ticket_requester_id DESC), 3) AS ticket_requester_id_cume_dist,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_channel, ticket_priority ORDER BY ticket_requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ticket_requester_id_avg_3,
        LEAD(ticket_requester_id) OVER w_ticket_channel_ticket_priori_2 AS next_ticket_requester_id,
        ROUND(CUME_DIST() OVER w_event_type_ticket_id_by_even, 3) AS event_at_cume_dist,
        RANK() OVER w_event_type_ticket_id_by_even AS event_at_rank,
        ROUND(PERCENT_RANK() OVER w_ticket_id_event_at_month_by, 3) AS event_at_percent_rank,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_channel, ticket_id)) AS distinct_event_type_in_ticket_channel_ticket_id,
        NTH_VALUE(ticket_requester_id, 2) OVER (PARTITION BY event_type, ticket_priority ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_requester_id,
        MAX(ticket_requester_id) OVER (PARTITION BY ticket_channel, ticket_priority ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_requester_id_so_far
    FROM with_parent_ticket
    WINDOW w_ticket_channel_ticket_priori_2 AS (PARTITION BY ticket_channel, ticket_priority ORDER BY ticket_requester_id DESC, id), w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at ASC), w_ticket_id_event_at_month_by AS (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(ticket_priority, '#', CAST(next_ticket_requester_id AS STRING)) AS ticket_priority_ref,
        CONCAT_WS(': ', event_type, ticket_priority) AS event_type_ticket_priority_label,
        CONCAT(ticket_priority, '#', CAST(ticket_requester_id AS STRING)) AS ticket_priority_ref_2
    FROM windowed
    WHERE ticket_priority <> 'P2'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_type,
        ticket_requester_id,
        ticket_requester_id_cume_dist,
        ticket_requester_id_avg_3,
        next_ticket_requester_id,
        event_at_cume_dist,
        event_at_rank,
        event_at_percent_rank,
        second_ticket_requester_id,
        max_ticket_requester_id_so_far,
        event_type_ticket_priority_label,
        SIZE(COLLECT_SET(ticket_priority) OVER w_event_at_rank_event_type) AS distinct_ticket_priority_in_event_at_rank_event_,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY distinct_event_type_in_ticket_channel_ticket_id), 3) AS distinct_event_type_in_ticket_channel_ticket_id_,
        DENSE_RANK() OVER (ORDER BY event_at_percent_rank DESC, id) AS event_at_percent_rank_dense_rank,
        FIRST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY distinct_event_type_in_ticket_channel_ticket_id ORDER BY second_ticket_requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_requester_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY distinct_event_type_in_ticket_channel_ticket_id, id), 3) AS distinct_event_type_in_ticket_channel_ticket_id__2,
        COUNT(*) OVER (PARTITION BY distinct_event_type_in_ticket_channel_ticket_id, event_at_rank) AS rows_in_distinct_event_type_in_ticket_channel_ti,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_event_type_in_ticket_channel_ticket_id ORDER BY second_ticket_requester_id NULLS LAST, id), 3) AS second_ticket_requester_id_cume_dist
    FROM annotated
    WINDOW w_event_at_rank_event_type AS (PARTITION BY event_at_rank, event_type)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        event_at_rank,
        SUM(event_at_percent_rank) AS total_event_at_percent_rank,
        SUM(event_at_rank) AS total_event_at_rank,
        SUM(rows_in_distinct_event_type_in_ticket_channel_ti) FILTER (WHERE event_type IS NOT NULL) AS rows_in_distinct_event_type_in_ticket_channel_ti_2,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM with_windows
    GROUP BY event_at_rank
    HAVING SUM(first_known_ticket_requester_id) > 2
)
-- Final projection
SELECT
    event_at_rank,
    total_event_at_percent_rank,
    total_event_at_rank,
    rows_in_distinct_event_type_in_ticket_channel_ti_2,
    FLOOR(event_at_rank / 2) * 2 AS event_at_rank_bucket
FROM rollup_base
WHERE total_event_at_percent_rank < 5.4

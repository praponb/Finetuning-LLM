WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        LENGTH(TRIM(event_type)) AS event_type_len_2
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type = 'escalated'
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        ticket_closed_at,
        ticket_requester_id,
        ticket_priority,
        LAST_VALUE(ticket_closed_at) OVER (ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY YEAR(event_at), ticket_id ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY ticket_closed_at DESC) AS ticket_closed_at_dense_rank,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY ticket_priority, event_type)) AS distinct_ticket_id_in_ticket_priority_event_type,
        SUM(ticket_requester_id) OVER (PARTITION BY YEAR(event_at), ticket_id ORDER BY ticket_requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_sum_3_prior,
        MAX(ticket_requester_id) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_3,
        COUNT(*) OVER (PARTITION BY ticket_priority, event_type) AS rows_in_ticket_priority_event_type,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY ticket_priority, event_type), 0), 4) AS ticket_requester_id_share_of_ticket_priority_eve,
        RANK() OVER (ORDER BY ticket_closed_at DESC, id) AS ticket_closed_at_rank,
        DENSE_RANK() OVER w_event_at_year_ticket_id_by_t_3 AS ticket_requester_id_dense_rank
    FROM enriched_ticket
    WINDOW w_event_at_year_ticket_id_by_t_3 AS (PARTITION BY YEAR(event_at), ticket_id ORDER BY ticket_requester_id DESC)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(ticket_priority), ' ', '_')) AS ticket_priority_slug
    FROM ranked
    WHERE distinct_ticket_id_in_ticket_priority_event_type BETWEEN 2 AND 9
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(ticket_priority) OVER w_distinct_ticket_id_in_ticket) AS distinct_ticket_priority_in_distinct_ticket_id_i,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY distinct_ticket_id_in_ticket_priority_event_type, ticket_requester_id_dense_rank)) AS distinct_event_type_in_distinct_ticket_id_in_tic,
        LEAD(first_known_ticket_closed_at) OVER (PARTITION BY seq_no, TO_DATE(ticket_closed_at) ORDER BY ticket_closed_at_rank DESC, id) AS next_first_known_ticket_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, ticket_priority ORDER BY ticket_requester_id_sum_3_prior NULLS LAST), 3) AS ticket_requester_id_sum_3_prior_cume_dist,
        COUNT(*) OVER (PARTITION BY distinct_ticket_id_in_ticket_priority_event_type, ticket_requester_id_dense_rank) AS rows_in_distinct_ticket_id_in_ticket_priority_ev
    FROM labeled
    WINDOW w_distinct_ticket_id_in_ticket AS (PARTITION BY distinct_ticket_id_in_ticket_priority_event_type, ticket_requester_id_dense_rank)
)
-- Final projection
SELECT
    id,
    event_type,
    ticket_closed_at,
    ticket_requester_id,
    ticket_priority,
    last_ticket_closed_at,
    ticket_closed_at_dense_rank,
    ticket_requester_id_sum_3_prior,
    rows_in_ticket_priority_event_type,
    ticket_requester_id_dense_rank,
    ticket_priority_slug,
    distinct_ticket_priority_in_distinct_ticket_id_i,
    distinct_event_type_in_distinct_ticket_id_in_tic,
    ticket_requester_id_sum_3_prior_cume_dist,
    ROUND((UNIX_TIMESTAMP(last_ticket_closed_at) - UNIX_TIMESTAMP(ticket_closed_at)) / 3600, 1) AS hours_ticket_closed_at_to_last_ticket_closed_at
FROM with_windows
WHERE ticket_requester_id_dense_rank > 3
ORDER BY ticket_requester_id DESC

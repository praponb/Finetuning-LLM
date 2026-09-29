WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id >= 38
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_clean,
        s.days_since_event_at,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat
    FROM recent_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_type, ticket_csat ORDER BY event_at ASC, id) AS event_at_seq,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY ticket_id, ticket_csat), 2) AS ticket_csat_vs_ticket_id_ticket_csat_avg,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_id, ticket_csat), 2) AS ticket_id_ticket_csat_avg_ticket_requester_id,
        NTILE(10) OVER w_ticket_csat_actor_by_ticket AS ticket_requester_id_decile,
        SIZE(COLLECT_SET(actor) OVER w_ticket_id) AS distinct_actor_in_ticket_id,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_id, ticket_csat) AS ticket_id_ticket_csat_max_days_since_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_csat, actor ORDER BY ticket_requester_id DESC NULLS FIRST, id) AS prev_event_at,
        RANK() OVER (PARTITION BY event_type, ticket_csat ORDER BY event_at ASC, id) AS event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_csat, MONTH(event_at) ORDER BY ticket_requester_id, id), 3) AS ticket_requester_id_cume_dist,
        NTILE(5) OVER (PARTITION BY ticket_csat, MONTH(event_at) ORDER BY ticket_requester_id, id) AS ticket_requester_id_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_csat, MONTH(event_at) ORDER BY event_at), 3) AS event_at_percent_rank,
        LAG(ticket_csat, 3) OVER (PARTITION BY event_type, ticket_csat ORDER BY event_at ASC, id) AS prev3_ticket_csat,
        MAX(ticket_requester_id) OVER (PARTITION BY ticket_csat, MONTH(event_at) ORDER BY event_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_4
    FROM with_details
    WINDOW w_ticket_csat_actor_by_ticket AS (PARTITION BY ticket_csat, actor ORDER BY ticket_requester_id DESC NULLS FIRST, id), w_ticket_id AS (PARTITION BY ticket_id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(ticket_requester_id / 5) * 5 AS ticket_requester_id_bucket,
        event_at_percent_rank - event_at_seq AS event_at_percent_rank_minus_event_at_seq,
        CASE WHEN event_type IN ('replied', 'resolved') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM windowed
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        days_since_event_at,
        ticket_requester_id,
        ticket_csat,
        event_at_seq,
        ticket_csat_vs_ticket_id_ticket_csat_avg,
        ticket_id_ticket_csat_avg_ticket_requester_id,
        distinct_actor_in_ticket_id,
        ticket_id_ticket_csat_max_days_since_event_at,
        prev_event_at,
        event_at_rank,
        ticket_requester_id_cume_dist,
        event_at_percent_rank,
        prev3_ticket_csat,
        ticket_requester_id_max_4,
        ticket_requester_id_bucket,
        event_type_group,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_at_seq ORDER BY prev3_ticket_csat NULLS LAST), 3) AS prev3_ticket_csat_percent_rank,
        SIZE(COLLECT_SET(event_type) OVER w_ticket_csat) AS distinct_event_type_in_ticket_csat,
        COUNT(*) OVER (PARTITION BY ticket_csat) AS rows_in_ticket_csat,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY ticket_csat)) AS distinct_ticket_id_in_ticket_csat,
        LEAD(prev3_ticket_csat) OVER w_event_at_seq_by_prev3_ticket_2 AS next_prev3_ticket_csat,
        LAST_VALUE(ticket_requester_id_quintile) OVER (PARTITION BY event_at_seq ORDER BY prev3_ticket_csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_requester_id_quintile
    FROM annotated
    WINDOW w_ticket_csat AS (PARTITION BY ticket_csat), w_event_at_seq_by_prev3_ticket_2 AS (PARTITION BY event_at_seq ORDER BY prev3_ticket_csat NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_type_clean,
    days_since_event_at,
    ticket_requester_id,
    ticket_csat,
    ticket_csat_vs_ticket_id_ticket_csat_avg,
    ticket_id_ticket_csat_avg_ticket_requester_id,
    distinct_actor_in_ticket_id,
    ticket_id_ticket_csat_max_days_since_event_at,
    event_at_rank,
    prev3_ticket_csat,
    ticket_requester_id_max_4,
    prev3_ticket_csat_percent_rank,
    next_prev3_ticket_csat,
    last_ticket_requester_id_quintile,
    FLOOR(event_at_seq / 2) * 2 AS event_at_seq_bucket,
    DAYOFWEEK(event_at) AS event_at_dayofweek
FROM windowed_2
ORDER BY id NULLS LAST
LIMIT 10

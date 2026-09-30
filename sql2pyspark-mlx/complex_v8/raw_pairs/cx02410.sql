WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_type <> 'assigned'
      AND TO_DATE(event_at) < DATE '2025-03-02'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_label,
        s.actor_clean,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id DESC) AS ticket_requester_id_dense_rank,
        LEAD(ticket_requester_id) OVER (PARTITION BY ticket_channel, ticket_id ORDER BY ticket_requester_id DESC, id) AS next_ticket_requester_id,
        NTH_VALUE(ticket_requester_id, 2) OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_requester_id,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY MONTH(ticket_opened_at))) AS distinct_event_type_in_ticket_opened_at_month,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY MONTH(ticket_opened_at)), 0), 4) AS ticket_requester_id_share_of_ticket_opened_at_mo,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id DESC, id), 3) AS ticket_requester_id_cume_dist,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER w_event_type_ticket_channel, 2) AS ticket_requester_id_vs_event_type_ticket_channel
    FROM enriched
    WINDOW w_event_type_ticket_channel AS (PARTITION BY event_type, ticket_channel)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_label,
        actor_clean,
        ticket_requester_id,
        ticket_opened_at,
        ticket_channel,
        next_ticket_requester_id,
        second_ticket_requester_id,
        distinct_event_type_in_ticket_opened_at_month,
        ticket_requester_id_share_of_ticket_opened_at_mo,
        ticket_requester_id_cume_dist,
        ticket_requester_id_vs_event_type_ticket_channel
    FROM ranked
    WHERE ticket_requester_id_dense_rank = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(ticket_requester_id) OVER (PARTITION BY YEAR(ticket_opened_at), ticket_channel ORDER BY ticket_requester_id_share_of_ticket_opened_at_mo ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_7,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY ticket_channel, ticket_id), 2) AS ticket_requester_id_vs_ticket_channel_ticket_id_,
        ROUND(next_ticket_requester_id / NULLIF(SUM(next_ticket_requester_id) OVER (PARTITION BY ticket_channel, ticket_id), 0), 4) AS next_ticket_requester_id_share_of_ticket_channel,
        COUNT(*) OVER w_ticket_channel_ticket_id AS rows_in_ticket_channel_ticket_id,
        LEAD(next_ticket_requester_id, 3) OVER (PARTITION BY YEAR(ticket_opened_at), ticket_channel ORDER BY ticket_requester_id_share_of_ticket_opened_at_mo ASC, id) AS next3_next_ticket_requester_id,
        ROUND(second_ticket_requester_id / NULLIF(SUM(second_ticket_requester_id) OVER (PARTITION BY ticket_channel, ticket_id), 0), 4) AS second_ticket_requester_id_share_of_ticket_chann
    FROM first_rows
    WINDOW w_ticket_channel_ticket_id AS (PARTITION BY ticket_channel, ticket_id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_at_label,
    ticket_opened_at,
    ticket_channel,
    next_ticket_requester_id,
    distinct_event_type_in_ticket_opened_at_month,
    ticket_requester_id_share_of_ticket_opened_at_mo,
    ticket_requester_id_cume_dist,
    ticket_requester_id_max_7,
    ticket_requester_id_vs_ticket_channel_ticket_id_,
    next_ticket_requester_id_share_of_ticket_channel,
    rows_in_ticket_channel_ticket_id,
    second_ticket_requester_id_share_of_ticket_chann,
    INITCAP(TRIM(event_at_label)) AS event_at_label_clean
FROM metrics
WHERE ticket_requester_id_cume_dist BETWEEN 0.1 AND 0.7

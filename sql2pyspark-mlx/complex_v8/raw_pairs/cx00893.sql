WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
      AND event_type <> 'replied'
),
latest AS (
    -- Keep the latest row per event_type, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM filtered_events
    ) ranked
    WHERE recency_rank = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        ticket_id,
        actor,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
    FROM latest
    GROUP BY 1, 2
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.requester_id AS ticket_requester_id
    FROM grouped s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        ticket_id,
        actor,
        actor_list,
        ticket_requester_id,
        RANK() OVER (PARTITION BY actor ORDER BY ticket_requester_id) AS ticket_requester_id_rank,
        ROUND(PERCENT_RANK() OVER w_actor_by_ticket_requester_id_2, 3) AS ticket_requester_id_percent_rank,
        ROUND(CUME_DIST() OVER w_actor_by_ticket_requester_id, 3) AS ticket_requester_id_cume_dist,
        COUNT(*) OVER (PARTITION BY distinct_ticket_ids) AS rows_in_distinct_ticket_ids
    FROM combined_details_ticket
    WINDOW w_actor_by_ticket_requester_id_2 AS (PARTITION BY actor ORDER BY ticket_requester_id, ticket_id, actor), w_actor_by_ticket_requester_id AS (PARTITION BY actor ORDER BY ticket_requester_id)
)
SELECT
    ticket_id,
    actor,
    CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
    CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group_2
FROM metrics
ORDER BY actor_group NULLS LAST

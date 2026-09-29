WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(event_type)) AS event_type_clean,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type <> 'escalated'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM cleaned s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'system'
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_id,
        s.event_event_at,
        t.channel AS ticket_channel,
        ti.event_at
    FROM exploded_detail_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN ticket_events ti ON ti.id = s.event_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_channel,
        SUM(CASE WHEN ticket_channel IS NOT NULL THEN 1 ELSE 0 END) AS n_ticket_channel_is_not_null,
        COUNT(DISTINCT event_id) AS distinct_event_ids,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN ticket_channel = 'phone' THEN 1 ELSE 0 END) AS n_ticket_channel_phone,
        ROUND(100 * COUNT(CASE WHEN ticket_channel NOT IN ('phone', 'web') THEN 1 END) / COUNT(*), 1) AS pct_ticket_channel_not_in_phone,
        COUNT(*) AS n_rows
    FROM enriched_ticket
    WHERE ticket_channel IN ('phone', 'web', 'chat')
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        ticket_channel,
        distinct_event_ids,
        distinct_ids,
        n_ticket_channel_phone,
        n_rows,
        distinct_event_ids - LAG(distinct_event_ids) OVER (ORDER BY pct_ticket_channel_not_in_phone ASC, ticket_channel) AS distinct_event_ids_change,
        MIN(n_ticket_channel_phone) OVER (PARTITION BY n_ticket_channel_is_not_null ORDER BY pct_ticket_channel_not_in_phone DESC, ticket_channel ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_n_ticket_channel_phone_so_far,
        ROUND(PERCENT_RANK() OVER w_n_ticket_channel_is_not_null_2, 3) AS pct_ticket_channel_not_in_phone_percent_rank
    FROM grouped
    WINDOW w_n_ticket_channel_is_not_null_2 AS (PARTITION BY n_ticket_channel_is_not_null ORDER BY pct_ticket_channel_not_in_phone DESC)
)
-- Final projection
SELECT
    ticket_channel,
    distinct_event_ids,
    distinct_ids,
    n_ticket_channel_phone,
    min_n_ticket_channel_phone_so_far,
    UPPER(TRIM(ticket_channel)) AS ticket_channel_clean
FROM with_trends
WHERE n_rows < 9
ORDER BY ticket_channel_clean

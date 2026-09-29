WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_prefix,
        s.event_at_hour,
        t.tags AS ticket_tags
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE SIZE(t.tags) >= 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        actor,
        event_at_hour,
        ticket_tags,
        ROUND(CUME_DIST() OVER w_event_type_actor_by_event_at, 3) AS event_at_hour_cume_dist,
        ROUND(AVG(event_at_hour) OVER (ORDER BY event_at_hour DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_at_hour_avg_4,
        event_at_hour - LAG(event_at_hour) OVER w_event_at_hour_ticket_id_by_e AS event_at_hour_change,
        ROUND(event_at_hour / NULLIF(SUM(event_at_hour) OVER (PARTITION BY actor), 0), 4) AS event_at_hour_share_of_actor,
        MAX(event_at_hour) OVER w_event_at_hour AS event_at_hour_max_event_at_hour,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at_hour), 3) AS event_at_hour_percent_rank,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY actor)) AS distinct_event_type_in_actor,
        FIRST_VALUE(event_at_hour) OVER (PARTITION BY event_at_hour, ticket_id ORDER BY event_at_hour ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_hour,
        RANK() OVER (ORDER BY event_at_hour DESC) AS event_at_hour_rank,
        ROUND(AVG(event_at_hour) OVER (PARTITION BY actor), 2) AS actor_avg_event_at_hour,
        ROW_NUMBER() OVER w_event_at_hour_ticket_id_by_e AS seq_no
    FROM with_details_ticket
    WINDOW w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at_hour DESC, id), w_event_at_hour_ticket_id_by_e AS (PARTITION BY event_at_hour, ticket_id ORDER BY event_at_hour ASC, id), w_event_at_hour AS (PARTITION BY event_at_hour)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE event_at_hour_rank <= 5
)
-- Final projection
SELECT
    id,
    actor,
    event_at_hour,
    ticket_tags,
    event_at_hour_avg_4,
    event_at_hour_change,
    event_at_hour_share_of_actor,
    event_at_hour_max_event_at_hour,
    event_at_hour_percent_rank,
    distinct_event_type_in_actor,
    event_at_hour_rank,
    actor_avg_event_at_hour,
    seq_no,
    ROUND(actor_avg_event_at_hour * 0.25, 2) AS actor_avg_event_at_hour_adj,
    UPPER(TRIM(actor)) AS actor_clean
FROM leaders
WHERE event_at_hour_share_of_actor <= 0.7
ORDER BY seq_no

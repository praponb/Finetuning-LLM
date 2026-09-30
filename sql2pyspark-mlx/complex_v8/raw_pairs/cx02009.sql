WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_type = 'assigned'
      AND ticket_id <= 38
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_clean,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        actor,
        actor_clean,
        ticket_tags,
        ticket_priority,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id), 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER w_event_at_year_ticket_id_by_e, 3) AS event_at_cume_dist_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(CUME_DIST() OVER w_event_at_to_date_ticket_id_b, 3) AS event_at_cume_dist_3,
        LEAD(event_at, 3) OVER w_event_at_to_date_ticket_id_b_2 AS next3_event_at,
        NTILE(3) OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_tercile,
        RANK() OVER w_event_at_year_ticket_id_by_e AS event_at_rank,
        LAG(event_at, 2) OVER (PARTITION BY YEAR(event_at), ticket_id ORDER BY event_at DESC, id) AS prev2_event_at
    FROM joined_ticket
    WINDOW w_event_at_year_ticket_id_by_e AS (PARTITION BY YEAR(event_at), ticket_id ORDER BY event_at DESC), w_event_at_to_date_ticket_id_b AS (PARTITION BY TO_DATE(event_at), ticket_id ORDER BY event_at), w_event_at_to_date_ticket_id_b_2 AS (PARTITION BY TO_DATE(event_at), ticket_id ORDER BY event_at, id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        actor,
        actor_clean,
        ticket_tags,
        ticket_priority,
        event_at_cume_dist,
        event_at_cume_dist_2,
        first_known_event_at,
        last_event_at,
        event_at_cume_dist_3,
        next3_event_at,
        event_at_tercile,
        prev2_event_at
    FROM metrics
    WHERE event_at_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_at_tercile, MONTH(last_event_at))) AS distinct_actor_in_event_at_tercile_last_event_at,
        FIRST_VALUE(next3_event_at) IGNORE NULLS OVER (ORDER BY last_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next3_event_at,
        MAX(event_at_cume_dist_3) OVER (ORDER BY last_event_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_at_cume_dist_3_max_2_prior,
        ROW_NUMBER() OVER (ORDER BY event_at_tercile ASC, id) AS event_at_tercile_seq,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at_tercile ASC, id), 3) AS event_at_tercile_percent_rank
    FROM first_rows
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    actor_clean,
    ticket_priority,
    event_at_cume_dist,
    event_at_cume_dist_2,
    first_known_event_at,
    last_event_at,
    event_at_cume_dist_3,
    event_at_tercile,
    distinct_actor_in_event_at_tercile_last_event_at,
    first_known_next3_event_at,
    event_at_cume_dist_3_max_2_prior,
    event_at_tercile_seq,
    LOWER(REGEXP_REPLACE(TRIM(ticket_priority), ' ', '_')) AS ticket_priority_slug,
    LEAST(event_at_cume_dist_3, event_at_cume_dist) AS least_event_at_cume_dist_3_event_at_cume_dist
FROM windowed

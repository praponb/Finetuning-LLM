WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags,
        su.agent_name
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_priority, YEAR(event_at_week) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC), 3) AS event_at_cume_dist,
        LEAD(event_at_week, 3) OVER w_ticket_channel_by_event_at_w AS next3_event_at_week,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(LAG(event_at_week) OVER (ORDER BY event_at_week, id))) / 60, 1) AS minutes_since_prev_event_at_week,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at_week, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at, id), 3) AS event_at_cume_dist_2,
        LAST_VALUE(event_at_week) IGNORE NULLS OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_week,
        ROUND(PERCENT_RANK() OVER w_actor_by_event_at, 3) AS event_at_percent_rank,
        LEAD(event_at_week, 3) OVER w_all_by_event_at_week AS next3_event_at_week_2,
        RANK() OVER (ORDER BY event_at_week, id) AS event_at_week_rank,
        LEAD(event_at) OVER w_all_by_event_at_week AS next_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM joined_ticket
    WINDOW w_ticket_channel_by_event_at_w AS (PARTITION BY ticket_channel ORDER BY event_at_week ASC, id), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at, id), w_all_by_event_at_week AS (ORDER BY event_at_week, id)
)
-- Final projection
SELECT
    id,
    event_at,
    event_at_week,
    event_at_label,
    ticket_channel,
    ticket_tags,
    first_known_event_at,
    event_at_cume_dist,
    next3_event_at_week,
    minutes_since_prev_event_at,
    event_at_cume_dist_2,
    next3_event_at_week_2,
    next_event_at,
    second_event_at,
    ROUND(event_at_cume_dist / NULLIF(event_at_week_rank, 0), 3) AS event_at_cume_dist_per_event_at_week_rank
FROM with_trends
ORDER BY next_event_at DESC, second_event_at DESC

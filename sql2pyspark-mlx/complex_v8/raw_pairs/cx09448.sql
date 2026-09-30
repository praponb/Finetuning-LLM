WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE (event_type IS NOT NULL OR HOUR(event_at) BETWEEN 6 AND 10)
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_label,
        t.tags AS ticket_tags,
        su.team AS agent_team
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at,
        LAG(event_at, 2) OVER (ORDER BY event_at DESC, id) AS prev2_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2,
        LEAD(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS next_event_at,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(3) OVER (ORDER BY event_at DESC, id) AS event_at_tercile
    FROM with_details_ticket
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_label,
        ticket_tags,
        agent_team,
        last_known_event_at,
        prev_event_at,
        prev2_event_at,
        last_known_event_at_2,
        next_event_at,
        event_at_dense_rank,
        MAX(event_at_cume_dist) OVER (PARTITION BY MONTH(prev2_event_at), event_at_dense_rank ORDER BY prev2_event_at ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_at_cume_dist_max_4,
        LAST_VALUE(event_at_cume_dist) IGNORE NULLS OVER (PARTITION BY MONTH(prev2_event_at), event_at_dense_rank ORDER BY prev2_event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_cume_dist,
        FIRST_VALUE(prev_event_at) IGNORE NULLS OVER (PARTITION BY event_at_dense_rank, event_type ORDER BY first_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_event_at,
        COUNT(*) OVER (PARTITION BY TO_DATE(last_known_event_at), YEAR(last_known_event_at_2)) AS rows_in_last_known_event_at_to_date_last_known_e,
        LAST_VALUE(event_at_cume_dist) OVER (PARTITION BY YEAR(last_known_event_at) ORDER BY event_at_dense_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_cume_dist,
        ROUND(AVG(event_at_tercile) OVER w_last_known_event_at_to_date, 2) AS last_known_event_at_to_date_last_known_event_at_
    FROM with_trends
    WINDOW w_last_known_event_at_to_date AS (PARTITION BY TO_DATE(last_known_event_at), YEAR(last_known_event_at_2))
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    event_at_label,
    ticket_tags,
    agent_team,
    last_known_event_at,
    prev2_event_at,
    last_known_event_at_2,
    next_event_at,
    event_at_dense_rank,
    last_known_event_at_cume_dist,
    first_known_prev_event_at,
    last_event_at_cume_dist
FROM metrics
ORDER BY last_known_event_at NULLS LAST, last_event_at_cume_dist DESC, id
LIMIT 20

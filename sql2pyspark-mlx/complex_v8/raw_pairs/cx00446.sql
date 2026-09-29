WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m_2
    FROM ticket_events
    WHERE event_type = 'reopened'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_plus_15m,
        s.event_at_plus_15m_2,
        t.agent_id AS ticket_agent_id,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        su.agent_name
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(event_at_plus_15m, 2) OVER (PARTITION BY ticket_id ORDER BY event_at_plus_15m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_15m,
        NTILE(4) OVER w_all_by_event_at AS event_at_quartile,
        NTH_VALUE(event_at_plus_15m, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_15m_2,
        LAST_VALUE(event_at_plus_15m_2) OVER (PARTITION BY ticket_id ORDER BY event_at_plus_15m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_plus_15m_2,
        NTILE(3) OVER (PARTITION BY ticket_id ORDER BY event_at_plus_15m ASC, id) AS event_at_plus_15m_tercile
    FROM with_details
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC, id)
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.ticket_agent_id
              AND t.requester_id BETWEEN 2 AND 20
        )
),
ticket_priority_stats AS (
    -- Median event_at_quartile per ticket_priority
    SELECT ticket_priority, PERCENTILE_APPROX(event_at_quartile, 0.5) AS median_event_at_quartile
    FROM without_matches
    GROUP BY ticket_priority
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_at_quartile,
        ROUND(s.event_at_quartile - g.median_event_at_quartile, 2) AS event_at_quartile_minus_median
    FROM without_matches s
    JOIN ticket_priority_stats g ON g.ticket_priority = s.ticket_priority
    WHERE s.event_at_quartile >= g.median_event_at_quartile
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_at_plus_15m_2,
    ticket_agent_id,
    ticket_tags,
    agent_name,
    event_at_quartile,
    second_event_at_plus_15m_2,
    last_event_at_plus_15m_2,
    event_at_plus_15m_tercile,
    median_event_at_quartile,
    event_at_quartile_minus_median,
    LOWER(TRIM(actor)) AS actor_clean
FROM compared
WHERE HOUR(last_event_at_plus_15m_2) BETWEEN 6 AND 12
ORDER BY event_at_plus_15m_tercile DESC

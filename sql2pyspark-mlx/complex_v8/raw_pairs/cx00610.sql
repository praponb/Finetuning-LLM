WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        tags,
        COALESCE(csat, 0) AS csat_filled,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE requester_id BETWEEN 4 AND 18
      AND channel <> 'phone'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.csat_filled,
        s.priority_slug,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM cleaned s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT COUNT(*) FILTER (WHERE t.event_type = 'created') FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count,
        (SELECT COUNT(*) FILTER (WHERE t.actor IS NOT NULL) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_actor_count
    FROM joined s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        closed_at,
        priority_slug,
        agent_team,
        agent_hired_on,
        event_event_type_count,
        event_actor_count,
        MIN(event_count) OVER w_csat_filled_event_event_type AS csat_filled_event_event_type_count_min_event_cou,
        ROUND(csat_filled - AVG(csat_filled) OVER (PARTITION BY csat_filled, event_event_type_count), 2) AS csat_filled_vs_csat_filled_event_event_type_coun
    FROM with_stats
    WINDOW w_csat_filled_event_event_type AS (PARTITION BY csat_filled, event_event_type_count)
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM with_trends s
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND HOUR(t.event_at) BETWEEN 9 AND 14
              AND t.event_type NOT IN ('resolved', 'reopened')
        )
)
SELECT DISTINCT requester_id, event_event_type_count
FROM without_matches
ORDER BY event_event_type_count, requester_id DESC NULLS FIRST

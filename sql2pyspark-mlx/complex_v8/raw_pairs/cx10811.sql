WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.requester_id) AS prior_requester_id_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(opened_at)) AS rows_in_opened_at_month,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY MONTH(opened_at))) AS distinct_agent_id_in_opened_at_month,
        MAX(prior_requester_id_3d) OVER w_opened_at_month AS opened_at_month_max_prior_requester_id_3d,
        LEAD(opened_at, 3) OVER (PARTITION BY prior_rows_3d, agent_id ORDER BY prior_rows_3d, id) AS next3_opened_at
    FROM matched
    WINDOW w_opened_at_month AS (PARTITION BY MONTH(opened_at))
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        COUNT(CASE WHEN actor IN ('agent', 'customer') THEN 1 END) AS event_actor_count,
        COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        COALESCE(t.event_count, 0) AS event_count
    FROM with_trends s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_3d,
    prior_requester_id_3d,
    distinct_agent_id_in_opened_at_month,
    opened_at_month_max_prior_requester_id_3d,
    first_event_at,
    event_actor_count,
    event_count,
    DATE_FORMAT(first_event_at, 'MMM yyyy') AS first_event_at_label,
    COALESCE(prior_rows_3d, 0) AS prior_rows_3d_filled
FROM with_stats
ORDER BY first_event_at DESC

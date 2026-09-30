WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.requester_id) AS prior_requester_id_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM joined_ranges s
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_12h,
        s.prior_requester_id_12h,
        s.last_event_at,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM with_stats s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    prior_rows_12h,
    prior_requester_id_12h,
    last_event_at,
    agent_team,
    ROUND(prior_rows_12h / NULLIF(prior_requester_id_12h, 0), 3) AS prior_rows_12h_per_prior_requester_id_12h
FROM with_parent_agent
ORDER BY prior_rows_12h DESC, last_event_at DESC NULLS FIRST, id
LIMIT 5

WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.csat) AS prior_csat_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.prior_rows_3d,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM matched s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IN ('escalated', 'created', 'replied')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.prior_rows_3d,
        s.event_id,
        s.event_type,
        s.event_actor,
        su.agent_name,
        su.team AS agent_team
    FROM line_level s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        prior_rows_3d,
        event_id,
        event_type,
        event_actor,
        agent_name,
        agent_team,
        CONCAT_WS('|', agent_team, agent_name) AS agent_team_agent_name_label,
        LOWER(REGEXP_REPLACE(TRIM(event_actor), ' ', '_')) AS event_actor_slug,
        CASE WHEN prior_rows_3d >= 7 THEN 'HIGH' WHEN prior_rows_3d >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS prior_rows_3d_tier
    FROM combined_details
    WHERE agent_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    agent_id,
    prior_rows_3d,
    event_id,
    agent_name,
    agent_team,
    agent_team_agent_name_label,
    event_actor_slug,
    prior_rows_3d_tier
FROM tagged
ORDER BY agent_name DESC

WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.priority_len,
        su.tier AS agent_tier
    FROM cleaned s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
derived AS (
    -- Label and score rows
    SELECT *, CONCAT(agent_tier, '#', CAST(id AS STRING)) AS agent_tier_ref
    FROM combined_details
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.agent_tier,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM derived s
    JOIN ticket_events t ON t.ticket_id = s.id
),
event_actor_stats AS (
    -- Median csat per event_actor
    SELECT event_actor, PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM with_children
    GROUP BY event_actor
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_csat, ROUND(s.csat - g.median_csat, 2) AS csat_minus_median
    FROM with_children s
    JOIN event_actor_stats g ON g.event_actor = s.event_actor
    WHERE s.csat >= g.median_csat
)
SELECT
    id,
    agent_id,
    csat,
    agent_tier,
    event_id,
    event_type,
    event_at,
    event_actor,
    median_csat,
    csat_minus_median,
    event_at + INTERVAL 1 DAYS AS event_at_plus_1d,
    median_csat + csat_minus_median AS median_csat_plus_csat_minus_median
FROM vs_group
ORDER BY event_id, event_actor DESC

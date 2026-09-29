WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        TO_DATE(opened_at) AS opened_at_date
    FROM tickets
    WHERE requester_id BETWEEN 2 AND 19
      AND opened_at < TIMESTAMP '2025-02-18 14:00:00'
),
joined AS (
    -- Join support_agents
    SELECT s.*, su.team AS agent_team, su.tier AS agent_tier
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        opened_at_date,
        agent_team,
        TO_DATE(closed_at) AS closed_at_date,
        csat + requester_id AS csat_plus_requester_id,
        COALESCE(csat, 0) AS csat_filled
    FROM joined
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.*,
        su.tier AS agent_tier,
        su.team AS agent_team_2,
        su.hired_on AS agent_hired_on
    FROM derived s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on >= DATE '2024-07-28'
)
SELECT id, measure, metric_value
FROM (
    SELECT id, csat_filled, csat_plus_requester_id
    FROM with_details_agent
) src
UNPIVOT (
    metric_value FOR measure IN (csat_filled, csat_plus_requester_id)
) unp

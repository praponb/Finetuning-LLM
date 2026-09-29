WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week,
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE priority NOT IN ('P2', 'P1')
),
unique_rows AS (
    -- Keep the latest row per agent_id, priority
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        opened_at_week,
        least_csat_requester_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY least_csat_requester_id DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.least_csat_requester_id,
        su.team AS agent_team
    FROM unique_rows s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team <> 'Billing'
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    channel,
    least_csat_requester_id,
    FLOOR(least_csat_requester_id / 5) * 5 AS least_csat_requester_id_bucket
FROM with_details_agent
ORDER BY closed_at, least_csat_requester_id_bucket NULLS LAST

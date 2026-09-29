WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE csat >= 2
),
latest AS (
    -- Keep the latest row per priority, agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        channel_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY csat DESC, id DESC) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(csat) OVER (PARTITION BY agent_id, csat) AS agent_id_csat_min_csat,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat, TO_DATE(closed_at) ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior
    FROM latest
),
latest_2 AS (
    -- Keep the latest row per priority, agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        channel_slug,
        agent_id_csat_min_csat,
        requester_id_avg_3_prior
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY csat DESC, id DESC) AS row_num
        FROM windowed
    ) ranked
    WHERE row_num = 1
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.agent_name
    FROM latest_2 s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on >= DATE '2024-07-09'
)
SELECT DISTINCT id, agent_name, priority
FROM enriched_agent
WHERE agent_id_csat_min_csat >= 21

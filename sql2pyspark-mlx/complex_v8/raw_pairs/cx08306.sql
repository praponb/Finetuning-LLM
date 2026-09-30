WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE priority = 'P4'
)
SELECT *
FROM (
    SELECT agent_id, priority, requester_id_per_csat
    FROM base
) src
PIVOT (
    SUM(requester_id_per_csat) AS total, COUNT(requester_id_per_csat) AS n
    FOR priority IN ('P4', 'P3')
)
ORDER BY agent_id

WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags
    FROM tickets
    WHERE priority IN ('P4', 'P1', 'P2')
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    tags,
    COALESCE(csat, 0) AS csat_filled,
    COALESCE(csat, 0) AS csat_filled_2
FROM recent_tickets
ORDER BY requester_id NULLS LAST, csat_filled_2 DESC NULLS FIRST

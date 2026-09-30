WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        opened_at + INTERVAL 2 HOURS AS opened_at_plus_2h
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-08'
)
SELECT channel
FROM scoped
EXCEPT
SELECT priority AS channel
FROM tickets
WHERE priority = 'P1'
ORDER BY channel

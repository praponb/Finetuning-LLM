WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat
    FROM tickets
    WHERE requester_id BETWEEN 3 AND 18
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(priority)) AS priority_clean,
        LOWER(TRIM(priority)) AS priority_clean_2
    FROM cleaned
    WHERE channel <> 'chat'
)
SELECT
    id,
    agent_id,
    priority,
    channel,
    csat,
    priority_clean,
    priority_clean_2
FROM scored
ORDER BY priority DESC

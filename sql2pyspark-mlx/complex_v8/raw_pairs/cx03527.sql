WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        TO_DATE(opened_at) AS opened_at_date,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
),
deduped AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        opened_at_date,
        channel_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY channel_len DESC, id DESC) AS row_num
        FROM cleaned_tickets
    ) ranked
    WHERE row_num = 1
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team, su.agent_name
    FROM deduped s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT *
FROM (
    SELECT opened_at, priority, csat
    FROM joined_agent
) src
PIVOT (
    MAX(csat)
    FOR priority IN ('P2', 'P4')
)

WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        CONCAT_WS('-', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE channel IS NOT NULL
),
per_group AS (
    -- Summarise per group
    SELECT
        priority,
        csat,
        MIN(opened_at) AS first_opened_at,
        ROUND(STDDEV(csat), 2) AS stddev_csat
    FROM scoped
    WHERE agent_id >= 9
    GROUP BY priority, csat
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT csat, first_opened_at, priority, stddev_csat
    FROM per_group
) src
PIVOT (
    COUNT(stddev_csat)
    FOR priority IN ('P4', 'P1', 'P3')
)
ORDER BY csat, first_opened_at

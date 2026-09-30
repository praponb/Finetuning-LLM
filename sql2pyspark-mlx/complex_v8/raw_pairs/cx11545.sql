WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 9 AND 13
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        UPPER(TRIM(channel)) AS channel_clean
    FROM cleaned_tickets
    WHERE days_since_opened_at >= 175
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(channel, 'ALL') AS channel,
    csat,
    agent_id,
    GROUPING_ID() AS gid,
    ROUND(100 * COUNT(CASE WHEN channel = 'web' THEN 1 END) / COUNT(*), 1) AS pct_channel_web,
    COUNT(*) AS row_count
FROM labeled
GROUP BY ROLLUP(channel, csat, agent_id)
ORDER BY channel NULLS LAST, csat NULLS LAST, agent_id NULLS LAST

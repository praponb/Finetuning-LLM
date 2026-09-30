WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        SIZE(tags) AS tags_count,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-02-02 03:00:00'
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.csat,
        s.channel_slug,
        su.agent_name
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 6
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        agent_name,
        TO_DATE(opened_at) AS opened_at_date,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week
    FROM with_parent_agent
    WHERE opened_at < TIMESTAMP '2025-02-18 13:00:00'
)
-- Final projection
SELECT
    id,
    requester_id,
    csat,
    opened_at_date,
    opened_at_week,
    LOWER(TRIM(agent_name)) AS agent_name_clean,
    COALESCE(csat, 0) AS csat_filled
FROM labeled
ORDER BY csat_filled DESC, csat DESC, id
LIMIT 5

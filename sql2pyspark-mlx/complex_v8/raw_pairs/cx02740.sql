WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        tags
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'bug')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        csat,
        priority,
        COUNT(*) AS cnt,
        ROUND(AVG(requester_id), 2) AS avg_requester_id
    FROM cleaned_tickets
    GROUP BY agent_id, csat, priority
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE avg_requester_id > (SELECT AVG(avg_requester_id) FROM stats)
)
SELECT
    agent_id,
    priority,
    avg_requester_id,
    CASE WHEN cnt >= 9 THEN 'HIGH' WHEN cnt >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM with_activity

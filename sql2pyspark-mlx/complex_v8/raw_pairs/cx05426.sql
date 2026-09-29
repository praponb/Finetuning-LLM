WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-15 08:00:00'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN priority_len >= 9 THEN 'large' WHEN priority_len >= 6 THEN 'medium' ELSE 'small' END AS priority_len_tier,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM scoped_tickets
    WHERE channel_or_unknown = 'email'
)
SELECT id, field, metric_value
FROM (
    SELECT id, csat, priority_len, requester_id
    FROM derived
) src
UNPIVOT (
    metric_value FOR field IN (csat, priority_len, requester_id)
) unp

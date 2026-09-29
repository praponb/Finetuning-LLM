WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at,
        ABS(requester_id - 8) AS requester_id_distance,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM filtered_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND ARRAY_CONTAINS(t.tags, 'upgrade')
        )
      AND s.agent_id IS NOT NULL
),
grouped AS (
    -- Summarise per group
    SELECT
        priority_or_unknown,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_ym,
        ROUND(STDDEV(requester_id), 2) AS stddev_requester_id,
        SUM(CASE WHEN channel = 'web' THEN days_since_opened_at ELSE 0 END) AS days_since_opened_at_channel_web,
        ROUND(100 * COUNT(CASE WHEN channel = 'email' THEN 1 END) / COUNT(*), 1) AS pct_channel_email,
        SUM(requester_id) AS total_requester_id
    FROM qualified
    WHERE channel NOT IN ('web', 'phone')
    GROUP BY priority_or_unknown, DATE_FORMAT(closed_at, 'yyyy-MM')
),
annotated AS (
    -- Add calculated columns
    SELECT *, ROUND(pct_channel_email * 0.9, 2) AS pct_channel_email_adj
    FROM grouped
    WHERE days_since_opened_at_channel_web <= 2450
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(total_requester_id / NULLIF(stddev_requester_id, 0), 3) AS total_requester_id_per_stddev_requester_id,
        LEAST(pct_channel_email_adj, total_requester_id) AS least_pct_channel_email_adj_total_requester_id
    FROM annotated
    WHERE days_since_opened_at_channel_web >= 1400
)
SELECT
    priority_or_unknown,
    closed_at_ym,
    stddev_requester_id,
    days_since_opened_at_channel_web,
    pct_channel_email,
    total_requester_id_per_stddev_requester_id,
    least_pct_channel_email_adj_total_requester_id,
    FLOOR(total_requester_id / 25) * 25 AS total_requester_id_bucket
FROM tagged
WHERE closed_at_ym IS NOT NULL
ORDER BY priority_or_unknown

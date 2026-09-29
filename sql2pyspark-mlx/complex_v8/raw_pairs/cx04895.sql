WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        MONTH(opened_at) AS opened_at_month,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
      AND agent_id <= 7
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        greatest_requester_id_csat,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM base
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        MIN(closed_at) AS first_ticket_closed_at,
        MIN(requester_id) AS min_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.first_ticket_closed_at,
        t.min_ticket_requester_id
    FROM scored s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(requester_id, min_ticket_requester_id) AS least_requester_id_min_ticket_requester_id,
        CASE WHEN channel IN ('web', 'email') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM profiled
    WHERE requester_id > 2
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, requester_id, greatest_requester_id_csat, least_requester_id_min_ticket_requester_id
    FROM annotated
) src
UNPIVOT (
    metric_value FOR metric_name IN (requester_id, greatest_requester_id_csat, least_requester_id_min_ticket_requester_id)
) unp
ORDER BY id, metric_name

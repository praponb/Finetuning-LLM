WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref,
        csat + requester_id AS csat_plus_requester_id,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM tickets
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat_plus_requester_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM joined s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(agent_hired_on, 'yyyy-MM') AS agent_hired_on_ym,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(csat_plus_requester_id) FILTER (WHERE priority IS NOT NULL) AS csat_plus_requester_id_priority_is_not_null,
        MAX(csat_plus_requester_id) AS max_csat_plus_requester_id,
        PERCENTILE_APPROX(csat_plus_requester_id, 0.5) AS median_csat_plus_requester_id
    FROM with_counts
    GROUP BY 1
)
SELECT agent_hired_on_ym, metric, reading
FROM (
    SELECT agent_hired_on_ym, csat_plus_requester_id_priority_is_not_null, distinct_ids
    FROM grouped
) src
UNPIVOT (
    reading FOR metric IN (csat_plus_requester_id_priority_is_not_null, distinct_ids)
) unp

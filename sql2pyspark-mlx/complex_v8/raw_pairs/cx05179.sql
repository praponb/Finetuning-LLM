WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE requester_id BETWEEN 4 AND 23
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        closed_at_label,
        channel_ref,
        tags_count,
        requester_id - LAG(requester_id) OVER (PARTITION BY agent_id, YEAR(opened_at) ORDER BY closed_at, id) AS requester_id_change,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_id, YEAR(opened_at) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id, csat), 2) AS requester_id_vs_agent_id_csat_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, YEAR(opened_at) ORDER BY closed_at), 3) AS closed_at_percent_rank
    FROM cleaned_tickets
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type IN ('replied', 'resolved', 'assigned')
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, closed_at_percent_rank, requester_id_vs_agent_id_csat_avg
    FROM with_activity
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (closed_at_percent_rank, requester_id_vs_agent_id_csat_avg)
) unp

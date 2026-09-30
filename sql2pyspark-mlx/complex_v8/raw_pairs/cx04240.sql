WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at
    FROM tickets
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, ROUND(AVG(csat), 2) AS avg_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.avg_ticket_csat
    FROM recent s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
enriched AS (
    -- Label and score rows
    SELECT *, CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM with_counts
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        days_since_closed_at,
        hours_closed_at_to_opened_at,
        avg_ticket_csat,
        DENSE_RANK() OVER (PARTITION BY csat ORDER BY avg_ticket_csat DESC, id) AS avg_ticket_csat_dense_rank,
        requester_id - LAG(requester_id) OVER (PARTITION BY agent_id, csat ORDER BY opened_at ASC, id) AS requester_id_change
    FROM enriched
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        SUM(CASE WHEN channel IS NOT NULL THEN 1 ELSE 0 END) AS n_channel_is_not_null,
        MIN(opened_at) AS first_opened_at,
        SUM(CASE WHEN channel = 'chat' THEN requester_id ELSE 0 END) AS requester_id_channel_chat,
        ROUND(STDDEV(days_since_closed_at), 2) AS stddev_days_since_closed_at
    FROM with_windows
    GROUP BY 1
    HAVING MAX(hours_closed_at_to_opened_at) > -1200.0
),
ticket_stats_2 AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) FILTER (WHERE priority IN ('P2', 'P4')) AS ticket_priority_count,
        MIN(csat) AS min_ticket_csat,
        MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count,
        t.min_ticket_csat,
        t.last_ticket_closed_at
    FROM summary s
    LEFT JOIN ticket_stats_2 t ON t.agent_id = s.agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(min_ticket_csat - AVG(min_ticket_csat) OVER w_min_ticket_csat_ticket_prior, 2) AS min_ticket_csat_vs_min_ticket_csat_ticket_priori,
        SUM(n_channel_is_not_null) OVER (PARTITION BY MONTH(last_ticket_closed_at), n_channel_is_not_null ORDER BY first_opened_at, agent_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_channel_is_not_null,
        LEAD(stddev_days_since_closed_at, 3) OVER (PARTITION BY ticket_priority_count ORDER BY first_opened_at DESC NULLS FIRST, agent_id) AS next3_stddev_days_since_closed_at
    FROM with_stats
    WINDOW w_min_ticket_csat_ticket_prior AS (PARTITION BY min_ticket_csat, ticket_priority_count)
)
SELECT
    agent_id,
    n_channel_is_not_null,
    requester_id_channel_chat,
    stddev_days_since_closed_at,
    ticket_priority_count,
    min_ticket_csat,
    last_ticket_closed_at,
    min_ticket_csat_vs_min_ticket_csat_ticket_priori,
    running_n_channel_is_not_null,
    ticket_priority_count + n_channel_is_not_null AS ticket_priority_count_plus_n_channel_is_not_null
FROM with_trends
WHERE n_channel_is_not_null <= 6
ORDER BY agent_id, ticket_priority_count_plus_n_channel_is_not_null DESC
